package rabbitmq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

func (mq *RabbitMQClient) inputQueueLoop(respCh <-chan amqp.Delivery, responseChannel chan<- amqp.Delivery, connectionErrorCh chan *amqp.Error, consumer string, q amqp.Queue, context context.Context) {
	for {
		select {
		case m := <-respCh:
			if m.Acknowledger == nil {
				mq.l.Error("Connection on '{_target}' by consumer '{_consumer}' (queue: {_queue}) nil acknowledger (retry asap...)", log.Data{
					"_target":   log.Str(mq.maskedTarget),
					"_consumer": log.Str(consumer),
					"_queue":    log.Str(q.Name),
				})
				return
			}
			responseChannel <- m
		case e := <-connectionErrorCh:
			if e != nil {
				mq.l.Error("Connection on '{_target}' by consumer '{_consumer}' (queue: {_queue}) closed: {_error} (retry asap...)", log.Data{
					"_target":   log.Str(mq.maskedTarget),
					"_consumer": log.Str(consumer),
					"_queue":    log.Str(q.Name),
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
