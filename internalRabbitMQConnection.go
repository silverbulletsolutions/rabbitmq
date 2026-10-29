package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

type internalRabbitMQConnection struct {
	l    log.Logger
	ctx  context.Context
	conn *amqp.Connection
	ch   *amqp.Channel

	connectionString string
	maskedTarget     string
	prefetchCount    int

	publicationRetry *Publication
}

func newInternalRabbitMQConnection(l log.Logger, ctx context.Context, connectionString string, maskedTarget string, prefetchCount int, publicationRetry *Publication) *internalRabbitMQConnection {
	return &internalRabbitMQConnection{l: l, ctx: ctx, connectionString: connectionString, maskedTarget: maskedTarget, prefetchCount: prefetchCount, publicationRetry: publicationRetry}
}

func (i *internalRabbitMQConnection) connect() error {
	i.l.Info("Dial to {_rabbitmq}...", log.Data{"_rabbitmq": log.Str(i.maskedTarget)})
	conn, err := amqp.DialConfig(i.connectionString, amqp.Config{
		Heartbeat: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("dial to '%s' error: %w", i.maskedTarget, err)
	}

	i.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel with '%s' error: %w", i.maskedTarget, err)
	}
	i.ch = ch

	if i.prefetchCount > 0 {
		err = i.ch.Qos(i.prefetchCount, 0, false)
		if err != nil {
			return fmt.Errorf("qos with '%s' error: %w", i.maskedTarget, err)
		}
	}

	i.l.Info("Dial to {_rabbitmq}... done", log.Data{"_rabbitmq": log.Str(i.maskedTarget)})

	return nil
}

func (i *internalRabbitMQConnection) cleanup() {
	i.l.Info("Cleanup on {_id}...", log.Data{"_id": log.Str(i.maskedTarget)})
	if i.ch != nil {
		_ = i.ch.Close()
		i.ch = nil
	}
	if i.conn != nil {
		_ = i.conn.Close()
		i.conn = nil
	}
}
