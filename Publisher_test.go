package rabbitmq

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

func TestPublisher_ShutdownWithoutBroker(t *testing.T) {
	logger := log.NewConsoleLogger(log.LogLevelTrace)

	mq := NewRabbitMQClient(logger, "127.0.0.1:1", 200*time.Millisecond, "test-client")
	p := mq.NewPublisher("publisher")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	err := p.Publish(ctx, Publication{Key: "test", Body: []byte("test")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish without connection: expected %v, got %v", context.DeadlineExceeded, err)
	} else {
		t.Log("Publish without connection: got expected deadline exceeded (it's OK)")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()

	err = p.Publish(ctx2, Publication{Key: "test2", Body: []byte("test2")})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("2nd Publish without connection: expected %v, got %v", context.DeadlineExceeded, err)
	} else {
		t.Log("2nd Publish without connection: got expected deadline exceeded (it's OK)")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	start := time.Now()
	err = p.Shutdown(shutdownCtx)
	if err != nil {
		t.Fatalf("Shutdown: unexpected error %v", err)
	} else {
		t.Log("Shutdown: expected no error (it's OK)")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Shutdown with nothing pending took %v", d)
	}

	err = p.Publish(context.Background(), Publication{Key: "test", Body: []byte("test")})
	if !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("Publish after Shutdown: expected %v, got %v", ErrPublisherClosed, err)
	} else {
		t.Logf("Publish after Shutdown: got expected error %v (it's OK)", ErrPublisherClosed)
	}

	err = p.Publish(context.Background(), Publication{Key: "test", Body: []byte("test")})
	if !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("2nd Publish after Shutdown: expected %v, got %v", ErrPublisherClosed, err)
	} else {
		t.Logf("2nd Publish after Shutdown: got expected error %v (it's OK)", ErrPublisherClosed)
	}

	err = p.Shutdown(shutdownCtx)
	if err != nil {
		t.Fatalf("second Shutdown: unexpected error %v", err)
	} else {
		t.Log("second Shutdown: no error (it's OK)")
	}

	select {
	case <-p.Done():
		t.Log("Done closed after Shutdown (it's OK)")
	default:
		t.Fatal("Done not closed after Shutdown")
	}
}

func TestPublisher_ShutdownSendsAll(t *testing.T) {
	logger := log.NewConsoleLogger(log.LogLevelInfo)

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	if err != nil {
		t.Skipf("RabbitMQ on localhost:5672 not available: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	queueName := "test.queue." + t.Name() + "_" + time.Now().Format("20060102_150405")
	if _, err := ch.QueueDeclare(queueName, false, false, true, false, nil); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = ch.QueueDelete(queueName, false, false, false) }()

	mq := NewRabbitMQClient(logger, "localhost:5672", time.Second, "test-client")
	p := mq.NewPublisher("publisher")

	const publishMessageCount = 1000
	for i := range publishMessageCount {
		if err := p.Publish(context.Background(), Publication{Key: queueName, Body: []byte(strconv.Itoa(i))}); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
		if (i+1)%100 == 0 {
			t.Logf("Published %d messages...", i+1)
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: unexpected error %v", err)
	}

	q, err := ch.QueueDeclarePassive(queueName, false, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if q.Messages != publishMessageCount {
		t.Fatalf("Expected %d messages in queue, got %d", publishMessageCount, q.Messages)
	}
}
