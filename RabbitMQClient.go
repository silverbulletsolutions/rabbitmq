package rabbitmq

import (
	"context"
	"fmt"
	"git.silverwhisper.pl/golang-common-modules/log"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
	"sync"
	"time"
)

type RabbitMQClient struct {
	l log.Logger

	connectionString string
	reconnectTime    time.Duration
	clientName       string

	clientNo     int
	clientNoLock sync.Mutex
}

func NewRabbitMQClient(l log.Logger, connectionString string, reconnectTime time.Duration, clientName string) *RabbitMQClient {
	cs := connectionString
	if strings.Index(strings.ToLower(cs), "amqp://") != 0 {
		cs = fmt.Sprintf("amqp://%s", cs)
	}

	return &RabbitMQClient{l: l, connectionString: cs, reconnectTime: reconnectTime, clientName: clientName}
}

type RabbitMQQueue interface {
	InitializeQueue(channel *amqp.Channel) (amqp.Queue, error)
}

func (mq *RabbitMQClient) getClientId(consumer string) string {

	mq.clientNoLock.Lock()
	clientId := mq.clientNo
	mq.clientNo++
	mq.clientNoLock.Unlock()

	return fmt.Sprintf("%s-%s#%d", mq.clientName, consumer, clientId)
}

func (mq *RabbitMQClient) Consume(context context.Context, queue RabbitMQQueue, consumer string, prefetchCount int, autoAck bool, args amqp.Table) <-chan amqp.Delivery {

	outgoingResponseChannel := make(chan amqp.Delivery)

	go func() {
		for {
			if context.Err() != nil {
				mq.l.Info("Consume '{_target}' by consumer '{_consumer}' stopped: {_error}", log.Data{
					"_target":   log.Str(mq.connectionString),
					"_consumer": log.Str(consumer),
					"_error":    log.Err(context.Err()),
				})
				close(outgoingResponseChannel)
				return
			}

			c := newInternalRabbitMQConnection(mq.l, context, mq.connectionString, prefetchCount)
			mq.l.Info("Connecting to `{_target}` by consumer '{_consumer}'...", log.Data{
				"_target":   log.Str(mq.connectionString),
				"_consumer": log.Str(consumer),
			})

			err := c.connect()
			if err != nil {
				mq.l.Error("Can't connect to '{_target}' by consumer '{_consumer}': {_error} (reconnect in {_reconnectTime}ms...)", log.Data{
					"_target":        log.Str(mq.connectionString),
					"_consumer":      log.Str(consumer),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(mq.reconnectTime.Milliseconds()),
				})
				c.cleanup()
				time.Sleep(mq.reconnectTime)
				continue
			}

			q, err := queue.InitializeQueue(c.ch)
			if err != nil {
				mq.l.Error("Initialize queue on '{_target}' by consumer '{_consumer}' error: {_error} (retry in {_reconnectTime}ms...)", log.Data{
					"_target":        log.Str(mq.connectionString),
					"_consumer":      log.Str(consumer),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(mq.reconnectTime.Milliseconds()),
				})
				c.cleanup()
				time.Sleep(mq.reconnectTime)
				continue
			}

			consumerId := mq.getClientId(consumer)

			connectionErrorCh := make(chan *amqp.Error)
			c.conn.NotifyClose(connectionErrorCh)

			respCh, err := c.ch.Consume(q.Name, consumerId, autoAck, false, false, false, args)

			mq.inputQueueLoop(respCh, outgoingResponseChannel, connectionErrorCh, consumer, q, context)

			c.cleanup()
		}
	}()

	return outgoingResponseChannel
}

func (mq *RabbitMQClient) inputQueueLoop(respCh <-chan amqp.Delivery, responseChannel chan<- amqp.Delivery, connectionErrorCh chan *amqp.Error, consumer string, q amqp.Queue, context context.Context) {
	for {
		select {
		case m := <-respCh:
			responseChannel <- m
		case e := <-connectionErrorCh:
			if e != nil {
				mq.l.Error("Connection on '{_target}' by consumer '{_consumer}' (queue: {_queue}) closed: {_error} (retry asap...)", log.Data{
					"_target":        log.Str(mq.connectionString),
					"_consumer":      log.Str(consumer),
					"_queue":         log.Str(q.Name),
					"_error":         log.Str(e.Error()),
					"_reconnectTime": log.Int(mq.reconnectTime.Milliseconds()),
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

type Publication struct {
	Exchange        string
	Key             string
	Headers         amqp.Table
	CorrelationId   string
	ContentEncoding string
	ContentType     string
	Body            []byte
}

func (mq *RabbitMQClient) Publish(context context.Context, publisher string) chan<- Publication {

	outgoingPublicationChannel := make(chan Publication)

	go func() {
		for {
			if context.Err() != nil {
				mq.l.Info("Publish '{_target}' by publisher '{_publisher}' stopped: {_error}", log.Data{
					"_target":    log.Str(mq.connectionString),
					"_publisher": log.Str(publisher),
					"_error":     log.Err(context.Err()),
				})
				close(outgoingPublicationChannel)
				return
			}

			c := newInternalRabbitMQConnection(mq.l, context, mq.connectionString, 0)
			mq.l.Info("Connecting to `{_target}` by publisher '{_publisher}'...", log.Data{
				"_target":    log.Str(mq.connectionString),
				"_publisher": log.Str(publisher),
			})

			err := c.connect()
			if err != nil {
				mq.l.Error("Can't connect to '{_target}' by publisher '{_consumer}': {_publisher} (reconnect in {_reconnectTime}ms...)", log.Data{
					"_target":        log.Str(mq.connectionString),
					"_publisher":     log.Str(publisher),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(mq.reconnectTime.Milliseconds()),
				})
				c.cleanup()
				time.Sleep(mq.reconnectTime)
				continue
			}

			producerId := mq.getClientId(publisher)

			connectionErrorCh := make(chan *amqp.Error)
			c.conn.NotifyClose(connectionErrorCh)

			mq.outputQueueLoop(outgoingPublicationChannel, connectionErrorCh, producerId, c, context)

			c.cleanup()
		}
	}()

	return outgoingPublicationChannel
}

func (mq *RabbitMQClient) outputQueueLoop(outgoingPublicationChannel chan Publication, connectionErrorCh chan *amqp.Error, publisher string, c *internalRabbitMQConnection, context context.Context) {
	for {
		select {
		case msg := <-outgoingPublicationChannel:
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
				mq.l.Error("Publish on '{_target}' by publisher '{_publisher}' error: {_error} (reconnect asap...)", log.Data{
					"_target":        log.Str(mq.connectionString),
					"_publisher":     log.Str(publisher),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(mq.reconnectTime.Milliseconds()),
				})
				// todo: we lost this message :-/
				return
			}
		case e := <-connectionErrorCh:
			if e != nil {
				mq.l.Error("Connection on '{_target}' by publisher '{_publisher}' closed: {_error} (reconnect asap...)", log.Data{
					"_target":        log.Str(mq.connectionString),
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

type internalRabbitMQConnection struct {
	l    log.Logger
	ctx  context.Context
	conn *amqp.Connection
	ch   *amqp.Channel

	connectionString string
	prefetchCount    int
}

func newInternalRabbitMQConnection(l log.Logger, ctx context.Context, connectionString string, prefetchCount int) *internalRabbitMQConnection {
	return &internalRabbitMQConnection{l: l, ctx: ctx, connectionString: connectionString, prefetchCount: prefetchCount}
}

func (i *internalRabbitMQConnection) connect() error {
	i.l.Info("Dial to {_rabbitmq}...", log.Data{"_rabbitmq": log.Str(i.connectionString)})
	conn, err := amqp.Dial(i.connectionString)
	if err != nil {
		return fmt.Errorf("dial to '%s' error: %w", i.connectionString, err)
	}

	i.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("channel with '%s' error: %w", i.connectionString, err)
	}
	i.ch = ch

	if i.prefetchCount > 0 {
		err = i.ch.Qos(i.prefetchCount, 0, false)
		if err != nil {
			return fmt.Errorf("qos with '%s' error: %w", i.connectionString, err)
		}
	}

	i.l.Info("Dial to {_rabbitmq}... done", log.Data{"_rabbitmq": log.Str(i.connectionString)})

	return nil
}

func (i *internalRabbitMQConnection) cleanup() {
	i.l.Info("Cleanup on {_id}...", log.Data{"_id": log.Str(i.connectionString)})
	if i.ch != nil {
		_ = i.ch.Close()
		i.ch = nil
	}
	if i.conn != nil {
		_ = i.conn.Close()
		i.conn = nil
	}
}
