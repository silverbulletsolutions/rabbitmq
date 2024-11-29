package rabbitmq

import (
	"context"
	"fmt"
	"git.silverwhisper.pl/golang-common-modules/log"
	amqp "github.com/rabbitmq/amqp091-go"
	"testing"
	"time"
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
