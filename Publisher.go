package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

// ErrPublisherClosed is returned by Publish when the publisher is shut down (or shutting down)
// and the message was not accepted.
var ErrPublisherClosed = errors.New("rabbitmq: publisher closed")

type Publication struct {
	Exchange        string
	Key             string
	Headers         amqp.Table
	CorrelationId   string
	ContentEncoding string
	ContentType     string
	Body            []byte
}

type Publisher struct {
	name                       string
	outgoingPublicationChannel chan Publication

	stopCtx context.Context // done - stop accepting new messages, send pending ones (stopped also by kill)
	stop    context.CancelFunc
	killCtx context.Context // done - abandon pending messages, close connection immediately
	kill    context.CancelFunc
	done    chan struct{} // closed only by publisher goroutine when it has finished

	closeErr error // written by publisher goroutine before done is closed
}

// Publish hands the message over to the publisher. Returns ErrPublisherClosed when the publisher
// is shut down, or ctx error when ctx is done before the message was accepted.
func (p *Publisher) Publish(ctx context.Context, publication Publication) error {
	select {
	case p.outgoingPublicationChannel <- publication:
		return nil
	case <-p.stopCtx.Done():
		return ErrPublisherClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown stops accepting new messages, sends pending ones (reconnecting if needed) and closes
// the connection gracefully. When ctx is done first, pending messages are dropped, connection is closed
// immediately and ctx error is returned. Safe to call multiple times.
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

func newPublisher(mq *RabbitMQClient, publisher string) *Publisher {
	killCtx, kill := context.WithCancel(context.Background())
	stopCtx, stop := context.WithCancel(killCtx)

	p := &Publisher{
		name:                       publisher,
		outgoingPublicationChannel: make(chan Publication),
		stopCtx:                    stopCtx,
		stop:                       stop,
		killCtx:                    killCtx,
		kill:                       kill,
		done:                       make(chan struct{}),
	}

	go func() {
		defer close(p.done)

		var publicationRetry *Publication = nil
		for {
			if p.killCtx.Err() != nil || (p.stopCtx.Err() != nil && publicationRetry == nil) {
				if publicationRetry != nil {
					mq.l.Error("Message '{correlationId}' to '{_target}' exchange '{_exchange}' dropped - publisher '{_publisher}' shutdown deadline exceeded", log.Data{
						"_target":       log.Str(mq.maskedTarget),
						"_publisher":    log.Str(publisher),
						"_exchange":     log.Str(publicationRetry.Exchange),
						"correlationId": log.Str(publicationRetry.CorrelationId),
					})
					p.closeErr = errors.Join(p.closeErr, fmt.Errorf("rabbitmq: message '%s' not sent", publicationRetry.CorrelationId))
				}
				mq.l.Info("Publish '{_target}' by publisher '{_publisher}' stopped", log.Data{
					"_target":    log.Str(mq.maskedTarget),
					"_publisher": log.Str(publisher),
				})
				return
			}

			// nothing pending - shutdown interrupts connecting / waiting; pending retry - keep trying until kill
			waitCtx := p.stopCtx
			if publicationRetry != nil {
				waitCtx = p.killCtx
			}

			c := newInternalRabbitMQConnection(mq.l, waitCtx, mq.connectionString, mq.maskedTarget, 0, publicationRetry)
			mq.l.Info("Connecting to `{_target}` by publisher '{_publisher}'...", log.Data{
				"_target":    log.Str(mq.maskedTarget),
				"_publisher": log.Str(publisher),
			})

			err := c.connect()
			if err != nil {
				delay := mq.reconnectDelay()
				mq.l.Error("Can't connect to '{_target}' by publisher '{_publisher}': {_error} (reconnect in {_reconnectTime}ms...)", log.Data{
					"_target":        log.Str(mq.maskedTarget),
					"_publisher":     log.Str(publisher),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(delay.Milliseconds()),
				})
				_ = c.cleanup()
				sleepCtx(waitCtx, delay)
				continue
			}

			// kill closes the socket - unblocks Publish blocked e.g. by broker flow control
			conn := c.conn
			stopForce := context.AfterFunc(p.killCtx, func() { _ = conn.CloseDeadline(time.Now()) })

			producerId := mq.getClientId(publisher)

			connectionErrorCh := make(chan *amqp.Error, 1)
			c.conn.NotifyClose(connectionErrorCh)

			graceful := mq.outputQueueLoop(p.outgoingPublicationChannel, connectionErrorCh, producerId, c, p.stopCtx)

			publicationRetry = c.publicationRetry
			closeErr := c.cleanup()
			stopForce()

			if p.stopCtx.Err() != nil {
				switch {
				case !graceful:
					p.closeErr = errors.Join(p.closeErr, errors.New("rabbitmq: connection lost during shutdown - recently published messages may be lost"))
				case closeErr != nil:
					// channel close waits for broker to process all previously published frames
					p.closeErr = errors.Join(p.closeErr, fmt.Errorf("rabbitmq: close channel: %w", closeErr))
				}
			}
		}
	}()

	return p
}

// outputQueueLoop publishes pending retry first and then messages from outgoingPublicationChannel
// until ctx is done (returns true) or publishing / connection fails (returns false).
func (mq *RabbitMQClient) outputQueueLoop(outgoingPublicationChannel chan Publication, connectionErrorCh chan *amqp.Error, publisher string, c *internalRabbitMQConnection, ctx context.Context) bool {
	if c.publicationRetry != nil {
		msg := *c.publicationRetry
		err := c.ch.Publish(msg.Exchange, msg.Key, false, false, amqp.Publishing{
			Headers:         msg.Headers,
			ContentType:     msg.ContentType,
			ContentEncoding: msg.ContentEncoding,
			DeliveryMode:    1,
			Priority:        0,
			CorrelationId:   msg.CorrelationId,
			ReplyTo:         "",
			Expiration:      "",
			MessageId:       "",
			Timestamp:       time.Time{},
			Type:            "",
			UserId:          "",
			AppId:           publisher,
			Body:            msg.Body,
		})
		if err != nil {
			mq.l.Error("Retry publish message '{correlationId}' to '{_target}' exchange '{_exchange}' by publisher '{_publisher}' error: {_error} (reconnect asap...)", log.Data{
				"_target":       log.Str(mq.maskedTarget),
				"_publisher":    log.Str(publisher),
				"_exchange":     log.Str(msg.Exchange),
				"_error":        log.Err(err),
				"correlationId": log.Str(msg.CorrelationId),
			})
			c.publicationRetry = &msg
			return false
		}
		mq.l.Info("Retry publish message '{correlationId}' to '{_target}' exchange '{_exchange}' by publisher '{_publisher}' done.", log.Data{
			"_target":       log.Str(mq.maskedTarget),
			"_publisher":    log.Str(publisher),
			"_exchange":     log.Str(msg.Exchange),
			"correlationId": log.Str(msg.CorrelationId),
		})
		c.publicationRetry = nil
	} else {
		mq.l.Debug("Retry publish - NO message retry to '{_target}' by publisher '{_publisher}'.", log.Data{
			"_target":    log.Str(mq.maskedTarget),
			"_publisher": log.Str(publisher),
		})
	}

	for {
		select {
		case msg := <-outgoingPublicationChannel:
			mq.l.Trace("Publish message {correlationId} to '{_target}' exchange '{_exchange}' by publisher '{_publisher}'...", log.Data{
				"_target":       log.Str(mq.maskedTarget),
				"_publisher":    log.Str(publisher),
				"_exchange":     log.Str(msg.Exchange),
				"correlationId": log.Str(msg.CorrelationId),
			})

			err := c.ch.Publish(msg.Exchange, msg.Key, false, false, amqp.Publishing{
				Headers:         msg.Headers,
				ContentType:     msg.ContentType,
				ContentEncoding: msg.ContentEncoding,
				DeliveryMode:    1,
				Priority:        0,
				CorrelationId:   msg.CorrelationId,
				ReplyTo:         "",
				Expiration:      "",
				MessageId:       "",
				Timestamp:       time.Time{},
				Type:            "",
				UserId:          "",
				AppId:           publisher,
				Body:            msg.Body,
			})
			if err != nil {
				mq.l.Error("Publish message '{correlationId}' to '{_target}' exchange '{_exchange}' by publisher '{_publisher}' error: {_error} (reconnect asap...)", log.Data{
					"_target":       log.Str(mq.maskedTarget),
					"_exchange":     log.Str(msg.Exchange),
					"_publisher":    log.Str(publisher),
					"_error":        log.Err(err),
					"correlationId": log.Str(msg.CorrelationId),
				})
				c.publicationRetry = &msg
				return false
			}

			c.publicationRetry = nil
		case e := <-connectionErrorCh:
			if e != nil {
				mq.l.Error("Connection to '{_target}' by publisher '{_publisher}' closed: {_error} (reconnect asap...)", log.Data{
					"_target":    log.Str(mq.maskedTarget),
					"_publisher": log.Str(publisher),
					"_error":     log.Str(e.Error()),
				})
			} else {
				mq.l.Warn("Nil in connection error channel (publish)", log.Data{})
			}
			return false
		case <-ctx.Done():
			return true
		}
	}
}
