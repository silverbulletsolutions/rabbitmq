package rabbitmq

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

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
