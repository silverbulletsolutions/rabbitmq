package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"git.silverbullet.pl/golang-common-modules/log"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestRabbitMQClient_Consume(t *testing.T) {

	logger := log.NewConsoleLogger(log.LogLevelTrace)

	mq := NewRabbitMQClient(logger, "localhost:5672", 1*time.Second, "test-client")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	inputEvents := mq.Consume(ctx, UseExistingQueue("archive.debug.queue"), "superConsumer", 10, true, nil)

	msgCount := 0
	totalBodySize := 0
	start := time.Now()

	for ev := range inputEvents {

		msgCount += 1
		totalBodySize += len(ev.Body)

		if d := time.Since(start); d >= time.Second {

			logger.Info("Processed {_count} in {_time}ms ({_msgPerSec} messages/s, {_kiBPerSec} KiB/s)", log.Data{
				"_count":     log.Int(msgCount),
				"_time":      log.Int(d.Milliseconds()),
				"_msgPerSec": log.Int(1000 * int64(msgCount) / d.Milliseconds()),
				"_kiBPerSec": log.Str(fmt.Sprintf("%0.2f", 1000*float32(totalBodySize)/1024/float32(d.Milliseconds()))),
			})

			start = time.Now()
			msgCount = 0
			totalBodySize = 0
		}
	}

	if d := time.Since(start); d.Milliseconds() > 0 {
		logger.Info("Processed {_count} in {_time}ms ({_msgPerSec} messages/s, {_kiBPerSec} KiB/s)", log.Data{
			"_count":     log.Int(msgCount),
			"_time":      log.Int(d.Milliseconds()),
			"_msgPerSec": log.Int(1000 * int64(msgCount) / d.Milliseconds()),
			"_kiBPerSec": log.Str(fmt.Sprintf("%0.2f", 1000*float32(totalBodySize)/1024/float32(d.Milliseconds()))),
		})
	}

	logger.Info("Done!", log.Data{})

	<-ctx.Done()

}

func TestRabbitMQClient_Consume_AckEach(t *testing.T) {

	logger := log.NewConsoleLogger(log.LogLevelTrace)

	mq := NewRabbitMQClient(logger, "localhost:5672", 1*time.Second, "test-client")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	inputEvents := mq.Consume(ctx, UseExistingQueue("archive.debug.queue"), "superConsumer", 50, false, nil)

	msgCount := 0
	totalBodySize := 0
	start := time.Now()

	for ev := range inputEvents {

		msgCount += 1
		totalBodySize += len(ev.Body)

		_ = ev.Ack(false)

		if d := time.Since(start); d >= time.Second {

			logger.Info("Processed {_count} in {_time}ms ({_msgPerSec} messages/s, {_kiBPerSec} KiB/s)", log.Data{
				"_count":     log.Int(msgCount),
				"_time":      log.Int(d.Milliseconds()),
				"_msgPerSec": log.Int(1000 * int64(msgCount) / d.Milliseconds()),
				"_kiBPerSec": log.Str(fmt.Sprintf("%0.2f", 1000*float32(totalBodySize)/1024/float32(d.Milliseconds()))),
			})

			start = time.Now()
			msgCount = 0
			totalBodySize = 0
		}
	}

	if d := time.Since(start); d.Milliseconds() > 0 {
		logger.Info("Processed {_count} in {_time}ms ({_msgPerSec} messages/s, {_kiBPerSec} KiB/s)", log.Data{
			"_count":     log.Int(msgCount),
			"_time":      log.Int(d.Milliseconds()),
			"_msgPerSec": log.Int(1000 * int64(msgCount) / d.Milliseconds()),
			"_kiBPerSec": log.Str(fmt.Sprintf("%0.2f", 1000*float32(totalBodySize)/1024/float32(d.Milliseconds()))),
		})
	}

	logger.Info("Done!", log.Data{})

	<-ctx.Done()

}

func TestRabbitMQClient_Consume_Ack(t *testing.T) {

	logger := log.NewConsoleLogger(log.LogLevelTrace)

	mq := NewRabbitMQClient(logger, "localhost:5672", 1*time.Second, "test-client")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	inputEvents := mq.Consume(ctx, UseExistingQueue("archive.debug.queue"), "superConsumer", 10, false, nil)

	msgCount := 0
	totalBodySize := 0
	start := time.Now()

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastMsg *amqp.Delivery = nil

	for {
		select {
		case ev := <-inputEvents:
			msgCount += 1
			totalBodySize += len(ev.Body)

			lastMsg = &ev

			if d := time.Since(start); d >= time.Second {

				logger.Info("Processed {_count} in {_time}ms ({_msgPerSec} messages/s, {_kiBPerSec} KiB/s)", log.Data{
					"_count":     log.Int(msgCount),
					"_time":      log.Int(d.Milliseconds()),
					"_msgPerSec": log.Int(1000 * int64(msgCount) / d.Milliseconds()),
					"_kiBPerSec": log.Str(fmt.Sprintf("%0.2f", 1000*float32(totalBodySize)/1024/float32(d.Milliseconds()))),
				})

				start = time.Now()
				msgCount = 0
				totalBodySize = 0
			}
		case <-ticker.C:
			if lastMsg != nil {
				err := lastMsg.Ack(true)
				if err != nil {
					fmt.Printf("Ack error: %v", err)
				}
			}
		case <-ctx.Done():
			goto continueProcess
		}
	}
continueProcess:

	if d := time.Since(start); d.Milliseconds() > 0 {
		logger.Info("Processed {_count} in {_time}ms ({_msgPerSec} messages/s, {_kiBPerSec} KiB/s)", log.Data{
			"_count":     log.Int(msgCount),
			"_time":      log.Int(d.Milliseconds()),
			"_msgPerSec": log.Int(1000 * int64(msgCount) / d.Milliseconds()),
			"_kiBPerSec": log.Str(fmt.Sprintf("%0.2f", 1000*float32(totalBodySize)/1024/float32(d.Milliseconds()))),
		})
	}

	logger.Info("Done!", log.Data{})

	<-ctx.Done()

}

func TestRabbitMQClient_Publish(t *testing.T) {
	logger := log.NewConsoleLogger(log.LogLevelTrace)

	mq := NewRabbitMQClient(logger, "localhost:5672", 1*time.Second, "test-client")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	out := mq.Publish(ctx, "super-publisher")

	out <- Publication{
		Key:  "test.queue",
		Body: []byte("hello world!"),
	}

}

type publishWithReconnectMsg struct {
	Id int `json:"id"`
}

func TestRabbitMQClient_PublishWithMultipleConsumers(t *testing.T) {
	logger := log.NewConsoleLogger(log.LogLevelTrace)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	mq := NewRabbitMQClient(logger, "genai.sbs.local:5672", 1*time.Second, "test-client")

	var queueName = "test.queue." + t.Name() + "_" + time.Now().Format("20060102_150405")

	queueConsumer := mq.Consume(ctx, UseTemporaryQueue(queueName).NoExclusive(), "superConsumer", 7, false, nil)

	var count atomic.Int64

	var wg sync.WaitGroup

	for id := range 5 {
		wg.Go(consumerWorker(logger, fmt.Sprintf("consumer-%d", id+1), queueConsumer, ctx, func(consumerName string, m publishWithReconnectMsg) {
			time.Sleep(time.Duration(m.Id%5+1) * 250 * time.Millisecond)
			count.Add(1)
		}))
	}

	time.Sleep(100 * time.Millisecond) // wait for initialization of queue

	out := mq.Publish(ctx, "super-publisher")

	const publishMessageCount int = 10

	for i := 0; i < publishMessageCount; i++ {

		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		body, _ := json.Marshal(publishWithReconnectMsg{Id: i})
		logger.Info("Sending message '{_body}'...", log.Data{"_body": log.Str(body)})
		out <- Publication{
			Key:  queueName,
			Body: body,
		}
		logger.Info("Sending message '{_body}'... done", log.Data{"_body": log.Str(body)})
	}

	wg.Wait()
	logger.Info("Done!", log.Data{})

	if count.Load() != int64(publishMessageCount) {
		t.Errorf("Expected %d messages, got %d", publishMessageCount, count.Load())
	} else {
		fmt.Printf("All messages received!\n")
	}
}

func consumerWorker(logger log.Logger, consumerName string, queueConsumer <-chan amqp.Delivery, ctx context.Context, f func(consumerName string, m publishWithReconnectMsg)) func() {
	return func() {
		logger.Info("Starting consumer loop...", log.Data{})
		for {
			select {
			case ev := <-queueConsumer:
				logger.Info("Received message: {_body}...", log.Data{"_body": log.Str(ev.Body), "consumer": log.Str(consumerName)})
				var m publishWithReconnectMsg
				_ = json.NewDecoder(bytes.NewReader(ev.Body)).Decode(&m)

				f(consumerName, m)

				logger.Info("Received message: {_body}... done", log.Data{"_body": log.Str(ev.Body), "consumer": log.Str(consumerName)})
				_ = ev.Ack(false)
			case <-ctx.Done():
				logger.Info("Consumer loop done", log.Data{"consumer": log.Str(consumerName)})
				return
			}
		}
	}
}

func TestRabbitMQClient_PublishWithReconnect(t *testing.T) {
	logger := log.NewConsoleLogger(log.LogLevelTrace)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	mqSend := NewRabbitMQClient(logger, "localhost:5672", 1*time.Second, "test-client")
	mqRecv := NewRabbitMQClient(logger, "genai.sbs.local:5672", 1*time.Second, "test-client")

	var queueName = "test.queue." + t.Name() + "_" + time.Now().Format("20060102_150405")

	queueConsumer := mqRecv.Consume(ctx, UseTemporaryQueue(queueName).NoExclusive(), "superConsumer", 7, false, nil)

	const publishMessageCount int = 20

	out := mqSend.Publish(ctx, "super-publisher")

	var count atomic.Int32
	var wg sync.WaitGroup

	wg.Go(consumerWorker(logger, "consumer", queueConsumer, ctx, func(consumerName string, m publishWithReconnectMsg) {
		count.Add(1)

		if m.Id >= publishMessageCount {
			return
		}

		time.Sleep(2 * time.Second)
		newId := m.Id + 1
		body, _ := json.Marshal(publishWithReconnectMsg{Id: newId})
		logger.Info("Sending message '{_body}'...", log.Data{"_body": log.Str(body)})
		out <- Publication{
			Key:           queueName,
			Body:          body,
			CorrelationId: fmt.Sprintf("publication-%d", newId),
		}
		logger.Info("Sending message '{_body}'... done", log.Data{"_body": log.Str(body)})

	}))

	time.Sleep(100 * time.Millisecond) // wait for initialization of queue

	body, _ := json.Marshal(publishWithReconnectMsg{Id: 1})
	logger.Info("Sending message '{_body}'...", log.Data{"_body": log.Str(body)})
	out <- Publication{
		Key:  queueName,
		Body: body,
	}
	logger.Info("Sending message '{_body}'... done", log.Data{"_body": log.Str(body)})

	wg.Wait()
	logger.Info("Done!", log.Data{})

	if count.Load() != int32(publishMessageCount) {
		t.Errorf("Expected %d messages, got %d", publishMessageCount, count.Load())
	} else {
		fmt.Printf("All messages received!\n")
	}
}

func TestRabbitMQClient_MessageLoop(t *testing.T) {
	logger := log.NewConsoleLogger(log.LogLevelTrace)

	mq := NewRabbitMQClient(logger, "localhost:5672", 1*time.Second, "test-client")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	inputEvents := mq.Consume(ctx, UseExistingQueue("archive.debug.queue"), "superConsumer", 10, true, nil)

	outEvents := mq.Publish(ctx, "publisher")

	msgCount := 0
	totalBodySize := 0
	start := time.Now()

	idx := 0

	for ev := range inputEvents {
		idx++

		outEvents <- Publication{
			Exchange:        "",
			Key:             "archive.debug.queue",
			Headers:         ev.Headers,
			CorrelationId:   fmt.Sprintf("%s_%d", ev.CorrelationId, idx),
			ContentEncoding: ev.ContentEncoding,
			ContentType:     ev.ContentType,
			Body:            ev.Body,
		}

		idx++
		outEvents <- Publication{
			Exchange:        "",
			Key:             "archive.debug.queue",
			Headers:         ev.Headers,
			CorrelationId:   fmt.Sprintf("%s_%d", ev.CorrelationId, idx),
			ContentEncoding: ev.ContentEncoding,
			ContentType:     ev.ContentType,
			Body:            ev.Body,
		}

		idx++
		outEvents <- Publication{
			Exchange:        "",
			Key:             "archive.debug.queue",
			Headers:         ev.Headers,
			CorrelationId:   fmt.Sprintf("%s_%d", ev.CorrelationId, idx),
			ContentEncoding: ev.ContentEncoding,
			ContentType:     ev.ContentType,
			Body:            ev.Body,
		}

		idx++
		outEvents <- Publication{
			Exchange:        "",
			Key:             "archive.debug.queue",
			Headers:         ev.Headers,
			CorrelationId:   fmt.Sprintf("%s_%d", ev.CorrelationId, idx),
			ContentEncoding: ev.ContentEncoding,
			ContentType:     ev.ContentType,
			Body:            ev.Body,
		}

		msgCount += 1
		totalBodySize += len(ev.Body)

		if d := time.Since(start); d >= time.Second {

			logger.Info("Processed {_count} in {_time}ms ({_msgPerSec} messages/s, {_kiBPerSec} KiB/s)", log.Data{
				"_count":     log.Int(msgCount),
				"_time":      log.Int(d.Milliseconds()),
				"_msgPerSec": log.Int(1000 * int64(msgCount) / d.Milliseconds()),
				"_kiBPerSec": log.Str(fmt.Sprintf("%0.2f", 1000*float32(totalBodySize)/1024/float32(d.Milliseconds()))),
			})

			start = time.Now()
			msgCount = 0
			totalBodySize = 0
		}
	}

	if d := time.Since(start); d.Milliseconds() > 0 {
		logger.Info("Processed {_count} in {_time}ms ({_msgPerSec} messages/s, {_kiBPerSec} KiB/s)", log.Data{
			"_count":     log.Int(msgCount),
			"_time":      log.Int(d.Milliseconds()),
			"_msgPerSec": log.Int(1000 * int64(msgCount) / d.Milliseconds()),
			"_kiBPerSec": log.Str(fmt.Sprintf("%0.2f", 1000*float32(totalBodySize)/1024/float32(d.Milliseconds()))),
		})
	}

	logger.Info("Done!", log.Data{})

	<-ctx.Done()

}
