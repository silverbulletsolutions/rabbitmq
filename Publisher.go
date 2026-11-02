package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

var (
	// ErrPublisherClosed - publisher is shut down (or shutting down) and the message was not accepted,
	// or Shutdown deadline expired before the message was sent / confirmed.
	ErrPublisherClosed = errors.New("rabbitmq: publisher closed")
	// ErrUnroutable - mandatory message could not be routed to any queue.
	ErrUnroutable = errors.New("rabbitmq: message unroutable")
	// ErrNacked - broker rejected (nacked) the message.
	ErrNacked = errors.New("rabbitmq: message nacked by broker")
	// ErrChannelClosed - broker closed the channel (e.g. publish to non-existing exchange) before the message
	// was confirmed; the message may or may not have been delivered.
	ErrChannelClosed = errors.New("rabbitmq: channel closed by broker, message not confirmed")
	// ErrConnectionBlocked - connection is blocked by broker (resource alarm); returned together with ctx error.
	ErrConnectionBlocked = errors.New("rabbitmq: connection blocked by broker")
)

// Mandatory flag of a published message. Zero value means "publisher default" (WithMandatory).
type Mandatory uint8

const (
	MandatoryDefault Mandatory = iota
	MandatoryOn
	MandatoryOff
)

type Publication struct {
	Exchange        string
	Key             string
	Headers         amqp.Table
	CorrelationId   string
	ContentEncoding string
	ContentType     string
	Body            []byte

	Mandatory Mandatory // MandatoryDefault - publisher default (WithMandatory)
}

const (
	defaultMaxInFlight = 256

	// publishIdHeader is added to mandatory messages (confirms mode) to match broker returns with publications
	publishIdHeader = "x-publish-id"
)

type publisherOptions struct {
	confirms    bool
	mandatory   bool
	maxInFlight int
}

type PublisherOption func(*publisherOptions)

// WithoutConfirms disables publisher confirms - Publish returns as soon as the message is written to the socket,
// so a message may be lost (without error) when the connection drops.
func WithoutConfirms() PublisherOption {
	return func(o *publisherOptions) { o.confirms = false }
}

// WithMandatory sets default mandatory flag (default: false). With confirms enabled, Publish of an unroutable
// mandatory message returns ErrUnroutable (without confirms it is only logged).
func WithMandatory(mandatory bool) PublisherOption {
	return func(o *publisherOptions) { o.mandatory = mandatory }
}

// WithMaxInFlight sets max number of sent but not yet confirmed messages (default: 256).
// New messages wait when the limit is reached.
func WithMaxInFlight(n int) PublisherOption {
	return func(o *publisherOptions) {
		if n > 0 {
			o.maxInFlight = n
		}
	}
}

type Publisher struct {
	mq   *RabbitMQClient
	name string
	opts publisherOptions

	requests chan *publishRequest

	stopCtx context.Context // done - stop accepting new messages, send pending ones (stopped also by kill)
	stop    context.CancelFunc
	killCtx context.Context // done - abandon pending messages, close connection immediately
	kill    context.CancelFunc
	done    chan struct{} // closed only by publisher goroutine when it has finished

	blockedReason atomic.Pointer[string] // not nil while connection is blocked by broker

	closeErr error // written by publisher goroutine before done is closed
	nextId   int64 // accessed only by publisher goroutine
}

type publishRequest struct {
	ctx       context.Context // request is dropped (not sent / resent) when done
	msg       Publication
	id        int64
	returnErr error
	result    chan error // buffered(1), exactly one reply per request
}

func (r *publishRequest) reply(err error) {
	r.result <- err
}

// Publish sends the message and waits for the result:
//   - nil - message confirmed by the broker (WithoutConfirms: written to the socket),
//   - ErrPublisherClosed - publisher shut down; message not sent (or not confirmed when Shutdown deadline expired),
//   - ErrUnroutable, ErrNacked, ErrChannelClosed - message rejected by the broker,
//   - ctx error (with ErrConnectionBlocked while connection is blocked) - caller gave up; message is not sent
//     if it has not been sent yet, otherwise it may or may not be delivered.
//
// While the connection is down Publish waits for reconnect. Messages not confirmed before connection loss
// are sent again after reconnect, so consumers may receive duplicates.
func (p *Publisher) Publish(ctx context.Context, publication Publication) error {
	result, err := p.submit(ctx, ctx, publication)
	if err != nil {
		return err
	}

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return p.ctxErr(ctx)
	}
}

// submit hands the message over to the publisher goroutine (waiting bounded by ctx) and returns channel with
// the result; the message is dropped when reqCtx is done before it is sent
func (p *Publisher) submit(ctx context.Context, reqCtx context.Context, publication Publication) (<-chan error, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.stopCtx.Err() != nil {
		return nil, ErrPublisherClosed
	}

	req := &publishRequest{ctx: reqCtx, msg: publication, result: make(chan error, 1)}
	select {
	case p.requests <- req:
		return req.result, nil
	case <-p.stopCtx.Done():
		return nil, ErrPublisherClosed
	case <-ctx.Done():
		return nil, p.ctxErr(ctx)
	}
}

func (p *Publisher) ctxErr(ctx context.Context) error {
	if reason := p.blockedReason.Load(); reason != nil {
		return fmt.Errorf("%w (%s): %w", ErrConnectionBlocked, *reason, ctx.Err())
	}
	return ctx.Err()
}

// Shutdown stops accepting new messages, sends (and waits for confirmation of) pending ones - reconnecting
// if needed - and closes the connection gracefully. When ctx is done first, pending messages get
// ErrPublisherClosed, connection is closed immediately and ctx error is returned. Safe to call multiple times.
func (p *Publisher) Shutdown(ctx context.Context) error {
	p.stop()
	select {
	case <-p.done:
		return p.closeErr
	case <-ctx.Done():
		p.kill()
		<-p.done
		return ctx.Err()
	}
}

// Done is closed when the publisher has finished (after Shutdown).
func (p *Publisher) Done() <-chan struct{} {
	return p.done
}

func newPublisher(mq *RabbitMQClient, publisher string, opts ...PublisherOption) *Publisher {
	o := publisherOptions{
		confirms:    true,
		maxInFlight: defaultMaxInFlight,
	}
	for _, opt := range opts {
		opt(&o)
	}

	killCtx, kill := context.WithCancel(context.Background())
	stopCtx, stop := context.WithCancel(killCtx)

	p := &Publisher{
		mq:       mq,
		name:     publisher,
		opts:     o,
		requests: make(chan *publishRequest),
		stopCtx:  stopCtx,
		stop:     stop,
		killCtx:  killCtx,
		kill:     kill,
		done:     make(chan struct{}),
	}

	go p.run()

	return p
}

func (p *Publisher) run() {
	defer close(p.done)

	var pending []*publishRequest // accepted, not sent yet (or to be resent after reconnect)
	for {
		pending = dropExpired(pending)

		if p.killCtx.Err() != nil {
			p.abandon(pending, nil)
			return
		}
		if p.stopCtx.Err() != nil && len(pending) == 0 {
			p.mq.l.Info("Publish '{_target}' by publisher '{_publisher}' stopped", log.Data{
				"_target":    log.Str(p.mq.maskedTarget),
				"_publisher": log.Str(p.name),
			})
			return
		}

		// nothing pending - shutdown interrupts connecting / waiting; pending messages - keep trying until kill
		waitCtx := p.stopCtx
		if len(pending) > 0 {
			waitCtx = p.killCtx
		}

		c := newInternalRabbitMQConnection(p.mq.l, waitCtx, p.mq.connectionString, p.mq.maskedTarget, 0)
		p.mq.l.Info("Connecting to `{_target}` by publisher '{_publisher}'...", log.Data{
			"_target":    log.Str(p.mq.maskedTarget),
			"_publisher": log.Str(p.name),
		})

		err := c.connect()
		if err != nil {
			delay := p.mq.reconnectDelay()
			p.mq.l.Error("Can't connect to '{_target}' by publisher '{_publisher}': {_error} (reconnect in {_reconnectTime}ms...)", log.Data{
				"_target":        log.Str(p.mq.maskedTarget),
				"_publisher":     log.Str(p.name),
				"_error":         log.Err(err),
				"_reconnectTime": log.Int(delay.Milliseconds()),
			})
			_ = c.cleanup()
			sleepCtx(waitCtx, delay)
			continue
		}

		var finished bool
		pending, finished = p.outputQueueLoop(c, pending)
		if finished {
			return
		}
	}
}

// dropExpired replies to requests abandoned by callers and returns the remaining ones
func dropExpired(pending []*publishRequest) []*publishRequest {
	return slices.DeleteFunc(pending, func(req *publishRequest) bool {
		if err := req.ctx.Err(); err != nil {
			req.reply(err)
			return true
		}
		return false
	})
}

// abandon fails all given requests with ErrPublisherClosed (Shutdown deadline exceeded)
func (p *Publisher) abandon(pending []*publishRequest, unconfirmed map[uint64]*publishRequest) {
	for _, req := range pending {
		req.reply(ErrPublisherClosed)
	}
	for _, req := range unconfirmed {
		req.reply(ErrPublisherClosed)
	}

	count := len(pending) + len(unconfirmed)
	if count > 0 {
		p.closeErr = errors.Join(p.closeErr, fmt.Errorf("rabbitmq: %d messages not sent or not confirmed", count))
	}
	p.mq.l.Warn("Publish '{_target}' by publisher '{_publisher}' stopped forcibly ({_count} messages not sent or not confirmed)", log.Data{
		"_target":    log.Str(p.mq.maskedTarget),
		"_publisher": log.Str(p.name),
		"_count":     log.Int(count),
	})
}

type publisherSession struct {
	c  *internalRabbitMQConnection
	id string

	confirms   chan amqp.Confirmation
	returns    chan amqp.Return
	chClosed   chan *amqp.Error
	connClosed chan *amqp.Error
	blockings  chan amqp.Blocking

	unconfirmed map[uint64]*publishRequest
	blocked     bool
}

// outputQueueLoop publishes messages on the connected session. Returns messages to be (re)sent after reconnect,
// or finished = true when the publisher is done (shut down gracefully or forcibly).
func (p *Publisher) outputQueueLoop(c *internalRabbitMQConnection, pending []*publishRequest) (_ []*publishRequest, finished bool) {
	s := &publisherSession{
		c:           c,
		id:          p.mq.getClientId(p.name),
		unconfirmed: make(map[uint64]*publishRequest),
	}

	// kill closes the socket - unblocks Publish blocked e.g. by broker flow control
	conn := c.conn
	stopForce := context.AfterFunc(p.killCtx, func() { _ = conn.CloseDeadline(time.Now()) })
	defer stopForce()
	defer p.blockedReason.Store(nil)

	if p.opts.confirms {
		if err := c.ch.Confirm(false); err != nil {
			p.mq.l.Error("Confirm mode on '{_target}' by publisher '{_publisher}' error: {_error} (reconnect asap...)", log.Data{
				"_target":    log.Str(p.mq.maskedTarget),
				"_publisher": log.Str(s.id),
				"_error":     log.Err(err),
			})
			_ = c.cleanup()
			return pending, false
		}
		// capacity >= max in-flight messages, so amqp091 never drops a confirmation (it gives up after 5s)
		s.confirms = c.ch.NotifyPublish(make(chan amqp.Confirmation, p.opts.maxInFlight))
	}
	s.returns = c.ch.NotifyReturn(make(chan amqp.Return, p.opts.maxInFlight))
	s.chClosed = c.ch.NotifyClose(make(chan *amqp.Error, 1))
	s.connClosed = c.conn.NotifyClose(make(chan *amqp.Error, 1))
	s.blockings = c.conn.NotifyBlocked(make(chan amqp.Blocking, 1))

	p.mq.l.Info("Publisher '{_publisher}' connected to '{_target}' ({_count} messages to send)", log.Data{
		"_target":    log.Str(p.mq.maskedTarget),
		"_publisher": log.Str(s.id),
		"_count":     log.Int(len(pending)),
	})

	for {
		for !s.blocked && len(pending) > 0 && len(s.unconfirmed) < p.opts.maxInFlight {
			req := pending[0]
			if err := req.ctx.Err(); err != nil {
				pending = pending[1:]
				req.reply(err)
				continue
			}
			if err := p.send(s, req); err != nil {
				p.mq.l.Error("Publish message '{correlationId}' to '{_target}' exchange '{_exchange}' by publisher '{_publisher}' error: {_error} (reconnect asap...)", log.Data{
					"_target":       log.Str(p.mq.maskedTarget),
					"_publisher":    log.Str(s.id),
					"_exchange":     log.Str(req.msg.Exchange),
					"_error":        log.Err(err),
					"correlationId": log.Str(req.msg.CorrelationId),
				})
				return p.connectionLost(s, pending, nil), false
			}
			pending = pending[1:]
		}

		stopping := p.stopCtx.Err() != nil
		if stopping && len(pending) == 0 && len(s.unconfirmed) == 0 {
			// channel close waits for broker to process all previously published frames
			if err := c.cleanup(); err != nil {
				p.closeErr = errors.Join(p.closeErr, fmt.Errorf("rabbitmq: close channel: %w", err))
			}
			p.mq.l.Info("Publish '{_target}' by publisher '{_publisher}' stopped", log.Data{
				"_target":    log.Str(p.mq.maskedTarget),
				"_publisher": log.Str(s.id),
			})
			return nil, true
		}

		var requests <-chan *publishRequest
		var stopCh <-chan struct{}
		if !stopping {
			stopCh = p.stopCtx.Done()
			if !s.blocked && len(pending) == 0 && len(s.unconfirmed) < p.opts.maxInFlight {
				requests = p.requests
			}
		}

		select {
		case req := <-requests:
			p.nextId++
			req.id = p.nextId
			pending = append(pending, req)

		case conf, ok := <-s.confirms:
			if !ok {
				s.confirms = nil // channel closed - reported by chClosed / connClosed
				continue
			}
			p.confirm(s, conf)

		case r, ok := <-s.returns:
			if !ok {
				s.returns = nil
				continue
			}
			p.returned(s, r)

		case b, ok := <-s.blockings:
			if !ok {
				s.blockings = nil
				continue
			}
			p.connectionBlocked(s, b)

		case e := <-s.chClosed:
			return p.connectionLost(s, pending, e), false

		case e := <-s.connClosed:
			return p.connectionLost(s, pending, e), false

		case <-stopCh:
			// re-evaluate: stop accepting new messages, finish pending ones

		case <-p.killCtx.Done():
			p.abandon(pending, s.unconfirmed)
			_ = c.cleanup()
			return nil, true
		}
	}
}

func (p *Publisher) send(s *publisherSession, req *publishRequest) error {
	msg := req.msg

	mandatory := p.opts.mandatory
	switch msg.Mandatory {
	case MandatoryOn:
		mandatory = true
	case MandatoryOff:
		mandatory = false
	}

	headers := msg.Headers
	if mandatory && p.opts.confirms {
		headers = make(amqp.Table, len(msg.Headers)+1)
		maps.Copy(headers, msg.Headers)
		headers[publishIdHeader] = req.id
	}

	p.mq.l.Trace("Publish message {correlationId} to '{_target}' exchange '{_exchange}' by publisher '{_publisher}'...", log.Data{
		"_target":       log.Str(p.mq.maskedTarget),
		"_publisher":    log.Str(s.id),
		"_exchange":     log.Str(msg.Exchange),
		"correlationId": log.Str(msg.CorrelationId),
	})

	dc, err := s.c.ch.PublishWithDeferredConfirm(msg.Exchange, msg.Key, mandatory, false, amqp.Publishing{
		Headers:         headers,
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    amqp.Transient,
		CorrelationId:   msg.CorrelationId,
		AppId:           s.id,
		Body:            msg.Body,
	})
	if err != nil {
		return err
	}

	if p.opts.confirms {
		s.unconfirmed[dc.DeliveryTag] = req
	} else {
		req.reply(nil)
	}
	return nil
}

func (p *Publisher) confirm(s *publisherSession, conf amqp.Confirmation) {
	// broker sends basic.return before basic.ack of the same message - handle returns first
	p.drainReturns(s)

	req, ok := s.unconfirmed[conf.DeliveryTag]
	if !ok {
		p.mq.l.Warn("Publisher '{_publisher}' got confirmation of unknown delivery tag {_tag}", log.Data{
			"_publisher": log.Str(s.id),
			"_tag":       log.Int(int64(conf.DeliveryTag)),
		})
		return
	}
	delete(s.unconfirmed, conf.DeliveryTag)

	switch {
	case !conf.Ack:
		req.reply(ErrNacked)
	case req.returnErr != nil:
		req.reply(req.returnErr)
	default:
		req.reply(nil)
	}
}

func (p *Publisher) drainReturns(s *publisherSession) {
	for {
		select {
		case r, ok := <-s.returns:
			if !ok {
				s.returns = nil
				return
			}
			p.returned(s, r)
		default:
			return
		}
	}
}

func (p *Publisher) returned(s *publisherSession, r amqp.Return) {
	if id, ok := r.Headers[publishIdHeader].(int64); ok {
		for _, req := range s.unconfirmed {
			if req.id == id {
				req.returnErr = fmt.Errorf("%w: %s (exchange '%s', key '%s')", ErrUnroutable, r.ReplyText, r.Exchange, r.RoutingKey)
				return
			}
		}
	}

	p.mq.l.Warn("Message '{correlationId}' published by '{_publisher}' to exchange '{_exchange}' with key '{_key}' returned: {_reason}", log.Data{
		"_publisher":    log.Str(s.id),
		"_exchange":     log.Str(r.Exchange),
		"_key":          log.Str(r.RoutingKey),
		"_reason":       log.Str(r.ReplyText),
		"correlationId": log.Str(r.CorrelationId),
	})
}

func (p *Publisher) connectionBlocked(s *publisherSession, b amqp.Blocking) {
	s.blocked = b.Active
	if b.Active {
		reason := b.Reason
		p.blockedReason.Store(&reason)
		p.mq.l.Warn("Connection '{_target}' of publisher '{_publisher}' blocked by broker: {_reason}", log.Data{
			"_target":    log.Str(p.mq.maskedTarget),
			"_publisher": log.Str(s.id),
			"_reason":    log.Str(b.Reason),
		})
	} else {
		p.blockedReason.Store(nil)
		p.mq.l.Info("Connection '{_target}' of publisher '{_publisher}' unblocked", log.Data{
			"_target":    log.Str(p.mq.maskedTarget),
			"_publisher": log.Str(s.id),
		})
	}
}

// connectionLost handles loss of channel / connection and returns messages to be sent after reconnect.
// After channel exception (e.g. publish to non-existing exchange) unconfirmed messages are failed instead of
// resent - one of them caused the exception and resending it would close the channel again.
func (p *Publisher) connectionLost(s *publisherSession, pending []*publishRequest, cause *amqp.Error) []*publishRequest {
	channelException := cause != nil && !s.c.conn.IsClosed()

	tags := slices.Sorted(maps.Keys(s.unconfirmed))
	action := "will be resent"
	if channelException {
		action = "failed"
		for _, tag := range tags {
			s.unconfirmed[tag].reply(fmt.Errorf("%w: %w", ErrChannelClosed, cause))
		}
	} else {
		resend := make([]*publishRequest, 0, len(tags)+len(pending))
		for _, tag := range tags {
			resend = append(resend, s.unconfirmed[tag])
		}
		pending = append(resend, pending...)
	}

	if !p.opts.confirms && p.stopCtx.Err() != nil {
		p.closeErr = errors.Join(p.closeErr, errors.New("rabbitmq: connection lost during shutdown - recently published messages may be lost"))
	}

	reason := "publish error"
	if cause != nil {
		reason = cause.Error()
	}
	p.mq.l.Error("Connection to '{_target}' by publisher '{_publisher}' lost: {_error} ({_unconfirmed} unconfirmed messages {_action}, reconnect asap...)", log.Data{
		"_target":      log.Str(p.mq.maskedTarget),
		"_publisher":   log.Str(s.id),
		"_error":       log.Str(reason),
		"_unconfirmed": log.Int(len(tags)),
		"_action":      log.Str(action),
	})

	_ = s.c.cleanup()
	return pending
}
