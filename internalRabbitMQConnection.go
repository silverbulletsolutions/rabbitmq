package rabbitmq

import (
	"context"
	"fmt"
	"net"
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
}

func newInternalRabbitMQConnection(l log.Logger, ctx context.Context, connectionString string, maskedTarget string, prefetchCount int) *internalRabbitMQConnection {
	return &internalRabbitMQConnection{l: l, ctx: ctx, connectionString: connectionString, maskedTarget: maskedTarget, prefetchCount: prefetchCount}
}

const dialTimeout = 30 * time.Second

func (i *internalRabbitMQConnection) connect() error {
	i.l.Info("Dial to {_rabbitmq}...", log.Data{"_rabbitmq": log.Str(i.maskedTarget)})

	// ctx interrupts TCP dial and TLS/AMQP handshake
	var stopHandshakeInterrupt func() bool
	conn, err := amqp.DialConfig(i.connectionString, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, addr string) (net.Conn, error) {
			d := net.Dialer{Timeout: dialTimeout}
			nc, err := d.DialContext(i.ctx, network, addr)
			if err != nil {
				return nil, err
			}
			// deadline for TLS and AMQP handshake (cleared by amqp091 when connection is established, as in amqp.DefaultDial)
			if err := nc.SetDeadline(time.Now().Add(dialTimeout)); err != nil {
				_ = nc.Close()
				return nil, err
			}
			stopHandshakeInterrupt = context.AfterFunc(i.ctx, func() { _ = nc.SetDeadline(time.Now()) })
			return nc, nil
		},
	})
	if stopHandshakeInterrupt != nil {
		stopHandshakeInterrupt()
	}
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

// cleanup closes channel and connection; returns channel close error (successful close of an open channel
// means the broker has processed all frames published on it before)
func (i *internalRabbitMQConnection) cleanup() error {
	i.l.Info("Cleanup on {_id}...", log.Data{"_id": log.Str(i.maskedTarget)})
	var err error
	if i.ch != nil {
		err = i.ch.Close()
		i.ch = nil
	}
	if i.conn != nil {
		_ = i.conn.Close()
		i.conn = nil
	}
	return err
}
