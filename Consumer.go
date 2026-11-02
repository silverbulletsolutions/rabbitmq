package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

var (
	// ErrConsumerClosed - consumer is shut down (or shutting down).
	ErrConsumerClosed = errors.New("rabbitmq: consumer closed")
	// ErrAlreadyAcknowledged - Ack / Nack of the delivery has already been called.
	ErrAlreadyAcknowledged = errors.New("rabbitmq: delivery already acknowledged")
)

// Handler processes a delivery. ctx is cancelled when Shutdown deadline expires.
//
// Unless the handler calls Ack / Nack itself, the delivery is acknowledged when the handler returns:
// nil - Ack, error - Nack without requeue (message is dead-lettered if the queue has DLX, otherwise dropped).
// Panic of the handler is recovered (as by net/http) and the message is requeued after WithPanicRequeueDelay.
// With WithAutoAck errors and panics are only logged.
type Handler interface {
	HandleDelivery(ctx context.Context, d *Delivery) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, d *Delivery) error

func (f HandlerFunc) HandleDelivery(ctx context.Context, d *Delivery) error {
	return f(ctx, d)
}

// Delivery is a received message. Ack / Nack can be called once (next calls return ErrAlreadyAcknowledged),
// before the handler returns; with WithAutoAck they do nothing.
type Delivery struct {
	Exchange    string
	RoutingKey  string
	Redelivered bool
	ConsumerTag string

	Headers         amqp.Table
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
	Body            []byte

	acknowledger amqp.Acknowledger
	tag          uint64
	autoAck      bool
	acknowledged atomic.Bool
	done         func() // called when the delivery has been handled
}

func newDelivery(m amqp.Delivery, autoAck bool, done func()) *Delivery {
	return &Delivery{
		Exchange:        m.Exchange,
		RoutingKey:      m.RoutingKey,
		Redelivered:     m.Redelivered,
		ConsumerTag:     m.ConsumerTag,
		Headers:         m.Headers,
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode:    m.DeliveryMode,
		Priority:        m.Priority,
		CorrelationId:   m.CorrelationId,
		ReplyTo:         m.ReplyTo,
		Expiration:      m.Expiration,
		MessageId:       m.MessageId,
		Timestamp:       m.Timestamp,
		Type:            m.Type,
		UserId:          m.UserId,
		AppId:           m.AppId,
		Body:            m.Body,
		acknowledger:    m.Acknowledger,
		tag:             m.DeliveryTag,
		autoAck:         autoAck,
		done:            done,
	}
}

// Ack acknowledges the message - broker removes it from the queue.
func (d *Delivery) Ack() error {
	if !d.acknowledged.CompareAndSwap(false, true) {
		return ErrAlreadyAcknowledged
	}
	if d.autoAck {
		return nil
	}
	return d.acknowledger.Ack(d.tag, false)
}

// Nack rejects the message - broker returns it to the queue (requeue) or dead-letters / drops it.
func (d *Delivery) Nack(requeue bool) error {
	if !d.acknowledged.CompareAndSwap(false, true) {
		return ErrAlreadyAcknowledged
	}
	if d.autoAck {
		return nil
	}
	return d.acknowledger.Nack(d.tag, false, requeue)
}

const defaultPanicRequeueDelay = time.Second

type consumerOptions struct {
	concurrency       int
	prefetch          int
	autoAck           bool
	args              amqp.Table
	panicRequeueDelay time.Duration
}

type ConsumerOption func(*consumerOptions)

// WithConcurrency sets number of handlers running concurrently (default: 1 - messages are processed in order).
func WithConcurrency(n int) ConsumerOption {
	return func(o *consumerOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// WithPrefetch sets max number of unacknowledged messages delivered to the consumer (default: concurrency).
// Must not be lower than concurrency. Ignored with WithAutoAck.
func WithPrefetch(n int) ConsumerOption {
	return func(o *consumerOptions) {
		if n > 0 {
			o.prefetch = n
		}
	}
}

// WithAutoAck - broker considers a message delivered as soon as it is sent (at-most-once): prefetch limit does not
// apply and messages received but not handled yet are lost on Shutdown, connection loss or crash.
func WithAutoAck() ConsumerOption {
	return func(o *consumerOptions) { o.autoAck = true }
}

// WithPanicRequeueDelay sets how long the worker waits before requeueing a message whose handler panicked
// (default: 1s). The delay limits the rate of retries (and logged errors) of a message that always panics;
// the worker is busy while waiting. Shutdown interrupts the delay - the message is requeued immediately.
func WithPanicRequeueDelay(d time.Duration) ConsumerOption {
	return func(o *consumerOptions) {
		if d >= 0 {
			o.panicRequeueDelay = d
		}
	}
}

// WithConsumerArgs sets arguments of basic.consume (e.g. x-priority).
func WithConsumerArgs(args amqp.Table) ConsumerOption {
	return func(o *consumerOptions) { o.args = args }
}

type Consumer struct {
	mq      *RabbitMQClient
	name    string
	queue   RabbitMQQueue
	handler Handler
	opts    consumerOptions

	mu      sync.Mutex
	started bool // protected by mu

	stopCtx context.Context // done - cancel subscription, finish handling of delivered messages (stopped also by kill)
	stop    context.CancelFunc
	killCtx context.Context // done - close connection immediately, cancel ctx of handlers
	kill    context.CancelFunc
	done    chan struct{} // closed when the consumer has finished (all handlers returned)
}

func newConsumer(mq *RabbitMQClient, name string, queue RabbitMQQueue, handler Handler, opts ...ConsumerOption) (*Consumer, error) {
	o := consumerOptions{concurrency: 1, panicRequeueDelay: defaultPanicRequeueDelay}
	for _, opt := range opts {
		opt(&o)
	}
	if o.prefetch == 0 {
		o.prefetch = o.concurrency
	}

	switch {
	case queue == nil:
		return nil, errors.New("rabbitmq: consumer queue is nil")
	case handler == nil:
		return nil, errors.New("rabbitmq: consumer handler is nil")
	case !o.autoAck && o.prefetch < o.concurrency:
		return nil, fmt.Errorf("rabbitmq: prefetch (%d) lower than concurrency (%d) - handlers would be idle", o.prefetch, o.concurrency)
	case !o.autoAck && o.prefetch > 65535:
		return nil, fmt.Errorf("rabbitmq: prefetch (%d) greater than 65535", o.prefetch)
	}

	killCtx, kill := context.WithCancel(context.Background())
	stopCtx, stop := context.WithCancel(killCtx)

	return &Consumer{
		mq:      mq,
		name:    name,
		queue:   queue,
		handler: handler,
		opts:    o,
		stopCtx: stopCtx,
		stop:    stop,
		killCtx: killCtx,
		kill:    kill,
		done:    make(chan struct{}),
	}, nil
}

// Start starts consuming in background (connecting and reconnecting as needed). Next calls do nothing;
// returns ErrConsumerClosed after Shutdown.
func (c *Consumer) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopCtx.Err() != nil {
		return ErrConsumerClosed
	}
	if !c.started {
		c.started = true
		go c.run()
	}
	return nil
}

// Shutdown cancels the subscription (broker stops delivering), waits for handlers to finish and acknowledge
// delivered messages and closes the connection. When ctx is done first, ctx of handlers is cancelled,
// connection is closed immediately (unacknowledged messages return to the queue) and ctx error is returned
// without waiting for handlers - Done is closed when they return. Safe to call multiple times.
func (c *Consumer) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	c.stop()
	if !c.started {
		c.started = true
		close(c.done)
	}
	c.mu.Unlock()

	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		c.kill()
		return ctx.Err()
	}
}

// Done is closed when the consumer has finished (after Shutdown, when all handlers have returned).
func (c *Consumer) Done() <-chan struct{} {
	return c.done
}

func (c *Consumer) run() {
	defer close(c.done)

	jobs := make(chan *Delivery)
	var workers sync.WaitGroup
	for range c.opts.concurrency {
		workers.Go(func() {
			for d := range jobs {
				c.handle(d)
			}
		})
	}
	defer workers.Wait()
	defer close(jobs)

	prefetch := c.opts.prefetch
	if c.opts.autoAck {
		prefetch = 0
	}

	for {
		if c.stopCtx.Err() != nil {
			c.mq.l.Info("Consumer '{_consumer}' of '{_target}' stopped", log.Data{
				"_target":   log.Str(c.mq.maskedTarget),
				"_consumer": log.Str(c.name),
			})
			return
		}

		conn := newInternalRabbitMQConnection(c.mq.l, c.stopCtx, c.mq.connectionString, c.mq.maskedTarget, prefetch)
		c.mq.l.Info("Connecting to `{_target}` by consumer '{_consumer}'...", log.Data{
			"_target":   log.Str(c.mq.maskedTarget),
			"_consumer": log.Str(c.name),
		})

		if err := conn.connect(); err != nil {
			c.retryLater(conn, "Connect", err)
			continue
		}

		q, err := c.queue.InitializeQueue(conn.ch)
		if err != nil {
			c.retryLater(conn, "Initialize queue", err)
			continue
		}

		tag := c.mq.getClientId(c.name)
		deliveries, err := conn.ch.Consume(q.Name, tag, c.opts.autoAck, false, false, false, c.opts.args)
		if err != nil {
			c.retryLater(conn, fmt.Sprintf("Consume queue '%s'", q.Name), err)
			continue
		}

		c.consumeLoop(conn, tag, q.Name, deliveries, jobs)
	}
}

func (c *Consumer) retryLater(conn *internalRabbitMQConnection, operation string, err error) {
	delay := c.mq.reconnectDelay()
	c.mq.l.Error("{_operation} on '{_target}' by consumer '{_consumer}' error: {_error} (retry in {_reconnectTime}ms...)", log.Data{
		"_operation":     log.Str(operation),
		"_target":        log.Str(c.mq.maskedTarget),
		"_consumer":      log.Str(c.name),
		"_error":         log.Err(err),
		"_reconnectTime": log.Int(delay.Milliseconds()),
	})
	_ = conn.cleanup()
	sleepCtx(c.stopCtx, delay)
}

// consumeLoop dispatches deliveries to handlers until the subscription ends (Shutdown, cancel by broker,
// channel / connection loss)
func (c *Consumer) consumeLoop(conn *internalRabbitMQConnection, tag string, queue string, deliveries <-chan amqp.Delivery, jobs chan<- *Delivery) {
	amqpConn, ch := conn.conn, conn.ch

	// kill closes the socket - unacknowledged messages return to the queue
	stopForce := context.AfterFunc(c.killCtx, func() { _ = amqpConn.CloseDeadline(time.Now()) })
	defer stopForce()
	// stop cancels the subscription - amqp091 passes already received messages and then closes deliveries
	stopConsume := context.AfterFunc(c.stopCtx, func() { _ = ch.Cancel(tag, false) })
	defer stopConsume()

	chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))
	connClosed := amqpConn.NotifyClose(make(chan *amqp.Error, 1))

	c.mq.l.Info("Consumer '{_consumer}' consuming queue '{_queue}' on '{_target}'", log.Data{
		"_target":   log.Str(c.mq.maskedTarget),
		"_consumer": log.Str(tag),
		"_queue":    log.Str(queue),
	})

	var inflight sync.WaitGroup
	var cause *amqp.Error
loop:
	for {
		select {
		case m, ok := <-deliveries:
			if !ok {
				break loop
			}
			if c.killCtx.Err() != nil {
				continue // connection is being closed - message returns to the queue
			}

			inflight.Add(1)
			select {
			case jobs <- newDelivery(m, c.opts.autoAck, inflight.Done):
			case cause = <-chClosed:
				inflight.Done()
				break loop
			case cause = <-connClosed:
				inflight.Done()
				break loop
			case <-c.killCtx.Done():
				inflight.Done()
			}

		case cause = <-chClosed:
			break loop
		case cause = <-connClosed:
			break loop
		}
	}

	if c.stopCtx.Err() != nil {
		if c.killCtx.Err() == nil {
			// handlers acknowledge their messages before the channel is closed
			inflight.Wait()
		}
		_ = conn.cleanup()
		return
	}

	if cause == nil {
		select {
		case cause = <-chClosed:
		case cause = <-connClosed:
		default:
		}
	}
	reason := "subscription cancelled by broker"
	if cause != nil {
		reason = cause.Error()
	}
	c.mq.l.Error("Consumer '{_consumer}' of queue '{_queue}' on '{_target}' interrupted: {_error} (reconnect asap...)", log.Data{
		"_target":   log.Str(c.mq.maskedTarget),
		"_consumer": log.Str(tag),
		"_queue":    log.Str(queue),
		"_error":    log.Str(reason),
	})
	_ = conn.cleanup()
}

func (c *Consumer) handle(d *Delivery) {
	defer d.done()

	err := c.callHandler(d)

	var ackErr error
	switch {
	case c.opts.autoAck || d.acknowledged.Load():
		if err != nil {
			c.logHandlerError(d, err, "")
		}
		return
	case err == nil:
		ackErr = d.Ack()
	case c.killCtx.Err() != nil:
		// forced shutdown - not rejected, message returns to the queue when connection is closed
		c.logHandlerError(d, err, " (message returns to the queue)")
		return
	case isPanic(err):
		c.logHandlerError(d, err, fmt.Sprintf(" (message requeued in %dms)", c.opts.panicRequeueDelay.Milliseconds()))
		sleepCtx(c.stopCtx, c.opts.panicRequeueDelay)
		if c.killCtx.Err() != nil {
			return // message returns to the queue when connection is closed
		}
		ackErr = d.Nack(true)
	default:
		c.logHandlerError(d, err, " (message rejected)")
		ackErr = d.Nack(false)
	}

	if ackErr != nil {
		c.mq.l.Warn("Acknowledge message '{correlationId}' by consumer '{_consumer}' error: {_error} (message will be redelivered)", log.Data{
			"_consumer":     log.Str(d.ConsumerTag),
			"_error":        log.Err(ackErr),
			"correlationId": log.Str(d.CorrelationId),
		})
	}
}

// PanicError - handler panicked; Stack is the stack trace of the panicking goroutine.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("handler panic: %v", e.Value)
}

func isPanic(err error) bool {
	_, ok := errors.AsType[*PanicError](err)
	return ok
}

func (c *Consumer) callHandler(d *Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return c.handler.HandleDelivery(c.killCtx, d)
}

func (c *Consumer) logHandlerError(d *Delivery, err error, action string) {
	data := log.Data{
		"_consumer":     log.Str(d.ConsumerTag),
		"_exchange":     log.Str(d.Exchange),
		"_error":        log.Err(err),
		"_action":       log.Str(action),
		"correlationId": log.Str(d.CorrelationId),
	}
	if pe, ok := errors.AsType[*PanicError](err); ok {
		data["stack"] = log.Str(string(pe.Stack))
	}
	c.mq.l.Error("Handler of consumer '{_consumer}' failed on message '{correlationId}' from exchange '{_exchange}': {_error}{_action}", data)
}

func (mq *RabbitMQClient) inputQueueLoop(respCh <-chan amqp.Delivery, responseChannel chan<- amqp.Delivery, connectionErrorCh chan *amqp.Error, consumer string, q amqp.Queue, context context.Context) {
	for {
		select {
		case m, ok := <-respCh:
			if !ok {
				// channel closed or subscription cancelled by broker (e.g. queue deleted)
				mq.l.Error("Consume on '{_target}' by consumer '{_consumer}' (queue: {_queue}) ended: channel closed or consumer cancelled by broker (retry asap...)", log.Data{
					"_target":   log.Str(mq.maskedTarget),
					"_consumer": log.Str(consumer),
					"_queue":    log.Str(q.Name),
				})
				return
			}
			select {
			case responseChannel <- m:
			case <-context.Done():
				return // message not acknowledged - returns to the queue when connection is closed
			}
		case e := <-connectionErrorCh:
			if e != nil {
				mq.l.Error("Connection on '{_target}' by consumer '{_consumer}' (queue: {_queue}) closed: {_error} (retry asap...)", log.Data{
					"_target":   log.Str(mq.maskedTarget),
					"_consumer": log.Str(consumer),
					"_queue":    log.Str(q.Name),
					"_error":    log.Str(e.Error()),
				})
			} else {
				mq.l.Warn("Nil in connection error (consume)", log.Data{})
			}
			return
		case <-context.Done():
			return
		}
	}
}
