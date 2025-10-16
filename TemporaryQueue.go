package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type TemporaryQueue struct {
	queueName string
	exclusive bool
	bindings  []TemporaryQueueBinding
	args      amqp.Table
}

func (q *TemporaryQueue) InitializeQueue(channel *amqp.Channel) (amqp.Queue, error) {

	queue, err := channel.QueueDeclare(q.queueName, false, true, q.exclusive, false, q.args)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("queue '%s' declare error: %w", q.queueName, err)
	}

	for _, binding := range q.bindings {
		err = channel.QueueBind(queue.Name, binding.key, binding.exchange, false, binding.args)
		if err != nil {
			return amqp.Queue{}, fmt.Errorf("queue '%s' binding to exchange '%s' (with args: %v) error: %w", q.queueName, binding.exchange, binding.args, err)
		}
	}

	return queue, nil
}

func UseTemporaryQueue(name string) *TemporaryQueue {
	return &TemporaryQueue{
		queueName: name,
		exclusive: true,
	}
}

func (q *TemporaryQueue) NoExclusive() *TemporaryQueue {
	q.exclusive = false
	return q
}

func (q *TemporaryQueue) AddArg(name string, value interface{}) *TemporaryQueue {
	if q.args == nil {
		q.args = make(amqp.Table, 2)
	}

	q.args[name] = value

	return q
}

type TemporaryQueueBinding struct {
	exchange string
	key      string
	args     amqp.Table
}

func (q *TemporaryQueue) Bind(exchangeName string, args amqp.Table) *TemporaryQueue {
	q.bindings = append(q.bindings, TemporaryQueueBinding{exchangeName, "", args})
	return q
}

func (q *TemporaryQueue) BindWithKey(exchangeName string, key string, args amqp.Table) *TemporaryQueue {
	q.bindings = append(q.bindings, TemporaryQueueBinding{exchangeName, key, args})
	return q
}
