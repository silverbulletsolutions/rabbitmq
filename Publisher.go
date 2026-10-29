package rabbitmq

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

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
	stopPublisher              context.CancelFunc
	publisherCtx               context.Context
	publisherShutdownChannel   chan struct{}
}

func (p *Publisher) Publish(ctx context.Context, publication Publication) error {
	select {
	case p.outgoingPublicationChannel <- publication:
	case <-p.publisherCtx.Done():
		return p.publisherCtx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (p *Publisher) Shutdown(ctx context.Context) {
	p.stopPublisher()
	select {
	case <-p.publisherShutdownChannel:
		// shutdown complete
	case <-ctx.Done():
		// may result panic
	}
	close(p.publisherShutdownChannel)
}

func newPublisher(mq *RabbitMQClient, publisher string) *Publisher {
	publisherCtx, stopPublisher := context.WithCancel(context.Background())

	p := &Publisher{
		publisherCtx:  publisherCtx,
		stopPublisher: stopPublisher,

		outgoingPublicationChannel: make(chan Publication),
		publisherShutdownChannel:   make(chan struct{}),
	}

	go func() {
		var publicationRetry *Publication = nil
		for {
			if publisherCtx.Err() != nil {
				mq.l.Info("Publish '{_target}' by publisher '{_publisher}' stopped: {_error}", log.Data{
					"_target":    log.Str(mq.maskedTarget),
					"_publisher": log.Str(publisher),
					"_error":     log.Err(publisherCtx.Err()),
				})
				p.publisherShutdownChannel <- struct{}{}
				return
			}

			c := newInternalRabbitMQConnection(mq.l, publisherCtx, mq.connectionString, mq.maskedTarget, 0, publicationRetry)
			mq.l.Info("Connecting to `{_target}` by publisher '{_publisher}'...", log.Data{
				"_target":    log.Str(mq.maskedTarget),
				"_publisher": log.Str(publisher),
			})

			err := c.connect()
			if err != nil {
				delay := mq.reconnectDelay()
				mq.l.Error("Can't connect to '{_target}' by publisher '{_consumer}': {_publisher} (reconnect in {_reconnectTime}ms...)", log.Data{
					"_target":        log.Str(mq.maskedTarget),
					"_publisher":     log.Str(publisher),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(delay.Milliseconds()),
				})
				c.cleanup()
				sleepCtx(publisherCtx, delay)
				continue
			}

			producerId := mq.getClientId(publisher)

			connectionErrorCh := make(chan *amqp.Error)
			c.conn.NotifyClose(connectionErrorCh)

			mq.outputQueueLoop(p.outgoingPublicationChannel, connectionErrorCh, producerId, c, publisherCtx)

			publicationRetry = c.publicationRetry
			c.cleanup()
		}
	}()

	return p
}

func (mq *RabbitMQClient) outputQueueLoop(outgoingPublicationChannel chan Publication, connectionErrorCh chan *amqp.Error, publisher string, c *internalRabbitMQConnection, context context.Context) {
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
			return
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
					"_target":        log.Str(mq.maskedTarget),
					"_exchange":      log.Str(msg.Exchange),
					"_publisher":     log.Str(publisher),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(mq.reconnectTime.Milliseconds()),
					"correlationId":  log.Str(msg.CorrelationId),
				})
				c.publicationRetry = &msg
				return
			}

			c.publicationRetry = nil
		case e := <-connectionErrorCh:
			if e != nil {
				mq.l.Error("Connection to '{_target}' by publisher '{_publisher}' closed: {_error} (reconnect asap...)", log.Data{
					"_target":        log.Str(mq.maskedTarget),
					"_publisher":     log.Str(publisher),
					"_error":         log.Str(e.Error()),
					"_reconnectTime": log.Int(mq.reconnectTime.Milliseconds()),
				})
			} else {
				mq.l.Warn("Nil in connection error channel (publish)", log.Data{})
			}
			return
		case <-context.Done():
			return
		}
	}
}
