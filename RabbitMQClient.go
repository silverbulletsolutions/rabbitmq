package rabbitmq

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

type RabbitMQClient struct {
	l log.Logger

	connectionString string
	maskedTarget     string // connectionString with masked password - safe for logging
	reconnectTime    time.Duration
	clientName       string

	clientNo     int
	clientNoLock sync.Mutex
}

func NewRabbitMQClient(l log.Logger, connectionString string, reconnectTime time.Duration, clientName string) *RabbitMQClient {
	cs := connectionString
	lcs := strings.ToLower(cs)
	if !strings.HasPrefix(lcs, "amqp://") && !strings.HasPrefix(lcs, "amqps://") {
		cs = fmt.Sprintf("amqp://%s", cs)
	}

	return &RabbitMQClient{l: l, connectionString: cs, maskedTarget: maskPassword(cs), reconnectTime: reconnectTime, clientName: clientName}
}

// maskPassword replaces password in connection string with '*' characters (one per password character)
func maskPassword(connectionString string) string {
	schemeEnd := strings.Index(connectionString, "://")
	if schemeEnd < 0 {
		return connectionString
	}
	rest := connectionString[schemeEnd+3:]

	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return connectionString
	}
	userInfo := rest[:at]

	colon := strings.Index(userInfo, ":")
	if colon < 0 {
		return connectionString
	}
	password := userInfo[colon+1:]
	if decoded, err := url.PathUnescape(password); err == nil {
		password = decoded
	}

	return connectionString[:schemeEnd+3] + userInfo[:colon+1] + strings.Repeat("*", utf8.RuneCountInString(password)) + rest[at:]
}

// reconnectDelay returns reconnectTime with random jitter (-25%..25% of reconnectTime added)
func (mq *RabbitMQClient) reconnectDelay() time.Duration {
	d := mq.reconnectTime
	if d > 0 {
		half := int64(d / 2)
		d += time.Duration(rand.Int64N(half+1) - half/2)
	}
	return d
}

// sleepCtx waits for given duration or until context is done
func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
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
					"_target":   log.Str(mq.maskedTarget),
					"_consumer": log.Str(consumer),
					"_error":    log.Err(context.Err()),
				})
				close(outgoingResponseChannel)
				return
			}

			c := newInternalRabbitMQConnection(mq.l, context, mq.connectionString, mq.maskedTarget, prefetchCount, nil)
			mq.l.Info("Connecting to `{_target}` by consumer '{_consumer}'...", log.Data{
				"_target":   log.Str(mq.maskedTarget),
				"_consumer": log.Str(consumer),
			})

			err := c.connect()
			if err != nil {
				delay := mq.reconnectDelay()
				mq.l.Error("Can't connect to '{_target}' by consumer '{_consumer}': {_error} (reconnect in {_reconnectTime}ms...)", log.Data{
					"_target":        log.Str(mq.maskedTarget),
					"_consumer":      log.Str(consumer),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(delay.Milliseconds()),
				})
				c.cleanup()
				sleepCtx(context, delay)
				continue
			}

			q, err := queue.InitializeQueue(c.ch)
			if err != nil {
				delay := mq.reconnectDelay()
				mq.l.Error("Initialize queue on '{_target}' by consumer '{_consumer}' error: {_error} (retry in {_reconnectTime}ms...)", log.Data{
					"_target":        log.Str(mq.maskedTarget),
					"_consumer":      log.Str(consumer),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(delay.Milliseconds()),
				})
				c.cleanup()
				sleepCtx(context, delay)
				continue
			}

			consumerId := mq.getClientId(consumer)

			connectionErrorCh := make(chan *amqp.Error)
			c.conn.NotifyClose(connectionErrorCh)

			respCh, err := c.ch.Consume(q.Name, consumerId, autoAck, false, false, false, args)
			if err != nil {
				delay := mq.reconnectDelay()
				mq.l.Error("Consume queue '{_queue}' on '{_target}' by consumer '{_consumer}' error: {_error} (retry in {_reconnectTime}ms...)", log.Data{
					"_target":        log.Str(mq.maskedTarget),
					"_consumer":      log.Str(consumer),
					"_queue":         log.Str(q.Name),
					"_error":         log.Err(err),
					"_reconnectTime": log.Int(delay.Milliseconds()),
				})
				c.cleanup()
				sleepCtx(context, delay)
				continue
			}

			mq.inputQueueLoop(respCh, outgoingResponseChannel, connectionErrorCh, consumer, q, context)

			c.cleanup()
		}
	}()

	return outgoingResponseChannel
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
		var publicationRetry *Publication = nil
		for {
			if context.Err() != nil {
				mq.l.Info("Publish '{_target}' by publisher '{_publisher}' stopped: {_error}", log.Data{
					"_target":    log.Str(mq.maskedTarget),
					"_publisher": log.Str(publisher),
					"_error":     log.Err(context.Err()),
				})
				close(outgoingPublicationChannel)
				return
			}

			c := newInternalRabbitMQConnection(mq.l, context, mq.connectionString, mq.maskedTarget, 0, publicationRetry)
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
				sleepCtx(context, delay)
				continue
			}

			producerId := mq.getClientId(publisher)

			connectionErrorCh := make(chan *amqp.Error)
			c.conn.NotifyClose(connectionErrorCh)

			mq.outputQueueLoop(outgoingPublicationChannel, connectionErrorCh, producerId, c, context)

			publicationRetry = c.publicationRetry
			c.cleanup()
		}
	}()

	return outgoingPublicationChannel
}
