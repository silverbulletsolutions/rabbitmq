package rabbitmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type UseExistingQueue string

func (p UseExistingQueue) InitializeQueue(channel *amqp.Channel) (amqp.Queue, error) {
	q, err := channel.QueueDeclarePassive(string(p), false, false, false, false, nil)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("passive queue '%s' error: %w", p, err)
	}
	return q, nil
}
