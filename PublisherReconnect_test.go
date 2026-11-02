package rabbitmq

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

const testBrokerAddr = "127.0.0.1:5672"

// testQueue declares a queue directly on the broker (skips the test when broker is not available)
func testQueue(t *testing.T) (*amqp.Channel, string) {
	conn, err := amqp.Dial("amqp://guest:guest@" + testBrokerAddr)
	if err != nil {
		t.Skipf("RabbitMQ on %s not available: %v", testBrokerAddr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}

	queueName := "test.queue." + t.Name() + "_" + time.Now().Format("20060102_150405.000")
	// exclusive - RabbitMQ 4.x does not permit transient non-exclusive queues
	if _, err := ch.QueueDeclare(queueName, false, false, true, false, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = ch.QueueDelete(queueName, false, false, false) })

	return ch, queueName
}

// queueMessageIds reads all messages from the queue and returns count of each message id (body)
func queueMessageIds(t *testing.T, ch *amqp.Channel, queueName string) map[int]int {
	ids := make(map[int]int)
	for {
		d, ok, err := ch.Get(queueName, true)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			return ids
		}
		id, err := strconv.Atoi(string(d.Body))
		if err != nil {
			t.Fatalf("unexpected message body %q", d.Body)
		}
		ids[id]++
	}
}

func newProxiedClient(t *testing.T) (*RabbitMQClient, *tcpProxy) {
	proxy := newTCPProxy(t, testBrokerAddr)
	mq := NewRabbitMQClient(log.NewConsoleLogger(log.LogLevelWarn), "guest:guest@"+proxy.Addr(), 100*time.Millisecond, "test-client")
	return mq, proxy
}

func publishId(ctx context.Context, p *Publisher, queueName string, id int) error {
	return p.Publish(ctx, Publication{Key: queueName, Body: []byte(strconv.Itoa(id)), CorrelationId: strconv.Itoa(id)})
}

// async runs f in background and returns channel with its result
func async(f func() error) <-chan error {
	result := make(chan error, 1)
	go func() { result <- f() }()
	return result
}

func waitResult(t *testing.T, name string, result <-chan error, timeout time.Duration) error {
	select {
	case err := <-result:
		return err
	case <-time.After(timeout):
		t.Fatalf("%s: no result within %v", name, timeout)
		return nil
	}
}

// Broker connection dropped several times while publishing concurrently - every Publish returning nil
// must be in the queue (duplicates allowed)
func TestPublisher_BrokerDropNoLoss(t *testing.T) {
	ch, queueName := testQueue(t)
	mq, proxy := newProxiedClient(t)
	p := mq.NewPublisher("publisher")

	const workers = 32
	const perWorker = 30

	publishing, stopDropping := context.WithCancel(context.Background())
	dropperDone := async(func() error {
		for range 5 {
			select {
			case <-time.After(400 * time.Millisecond):
				proxy.Drop()
			case <-publishing.Done():
				return nil
			}
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				if err := publishId(ctx, p, queueName, w*perWorker+i); err != nil {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	stopDropping()
	<-dropperDone
	close(errs)
	for err := range errs {
		t.Errorf("Publish error: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: unexpected error %v", err)
	}

	ids := queueMessageIds(t, ch, queueName)
	duplicates := 0
	for id := range workers * perWorker {
		switch n := ids[id]; {
		case n == 0:
			t.Errorf("message %d confirmed but missing in queue", id)
		case n > 1:
			duplicates += n - 1
		}
	}
	t.Logf("%d messages confirmed, %d in queue (%d duplicates)", workers*perWorker, len(ids)+duplicates, duplicates)
}

// Message sent into a dead link (not confirmed) is resent after reconnect - also during Shutdown
func TestPublisher_ShutdownResendsAfterReconnect(t *testing.T) {
	ch, queueName := testQueue(t)
	mq, proxy := newProxiedClient(t)
	p := mq.NewPublisher("publisher")

	if err := publishId(context.Background(), p, queueName, 0); err != nil {
		t.Fatalf("Publish 0: %v", err)
	}

	proxy.SetFrozen(true)
	published := async(func() error { return publishId(context.Background(), p, queueName, 1) })
	time.Sleep(300 * time.Millisecond) // message 1 sent into the dead link

	proxy.SetDown(true)
	proxy.Drop()
	proxy.SetFrozen(false)

	shutdown := async(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return p.Shutdown(ctx)
	})

	time.Sleep(time.Second) // publisher keeps reconnecting during Shutdown
	proxy.SetDown(false)

	if err := waitResult(t, "Publish 1", published, 10*time.Second); err != nil {
		t.Fatalf("Publish 1: unexpected error %v", err)
	}
	if err := waitResult(t, "Shutdown", shutdown, 10*time.Second); err != nil {
		t.Fatalf("Shutdown: unexpected error %v", err)
	}

	ids := queueMessageIds(t, ch, queueName)
	if ids[0] != 1 || ids[1] != 1 || len(ids) != 2 {
		t.Fatalf("expected messages 0 and 1 once, got %v", ids)
	}
}

// Broker does not come back before Shutdown deadline - pending message fails with ErrPublisherClosed
func TestPublisher_ShutdownDeadlineWithPending(t *testing.T) {
	ch, queueName := testQueue(t)
	mq, proxy := newProxiedClient(t)
	p := mq.NewPublisher("publisher")

	if err := publishId(context.Background(), p, queueName, 0); err != nil {
		t.Fatalf("Publish 0: %v", err)
	}

	proxy.SetFrozen(true)
	published := async(func() error { return publishId(context.Background(), p, queueName, 1) })
	time.Sleep(300 * time.Millisecond)

	proxy.SetDown(true)
	proxy.Drop()

	const deadline = 1500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()

	start := time.Now()
	if err := p.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown: expected %v, got %v", context.DeadlineExceeded, err)
	}
	if d := time.Since(start); d > deadline+time.Second {
		t.Fatalf("Shutdown took %v (deadline %v)", d, deadline)
	}

	if err := waitResult(t, "Publish 1", published, time.Second); !errors.Is(err, ErrPublisherClosed) {
		t.Fatalf("Publish 1: expected %v, got %v", ErrPublisherClosed, err)
	}

	ids := queueMessageIds(t, ch, queueName)
	if ids[0] != 1 || len(ids) != 1 {
		t.Fatalf("expected only message 0, got %v", ids)
	}
}

// Caller gave up (ctx done) before the message was confirmed - it must not be resent after reconnect
func TestPublisher_ExpiredMessageNotResent(t *testing.T) {
	ch, queueName := testQueue(t)
	mq, proxy := newProxiedClient(t)
	p := mq.NewPublisher("publisher")

	if err := publishId(context.Background(), p, queueName, 0); err != nil {
		t.Fatalf("Publish 0: %v", err)
	}

	proxy.SetFrozen(true)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := publishId(ctx, p, queueName, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish 1: expected %v, got %v", context.DeadlineExceeded, err)
	}

	proxy.Drop()
	proxy.SetFrozen(false)

	if err := publishId(context.Background(), p, queueName, 2); err != nil {
		t.Fatalf("Publish 2: %v", err)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := p.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: unexpected error %v", err)
	}

	ids := queueMessageIds(t, ch, queueName)
	if ids[0] != 1 || ids[2] != 1 || len(ids) != 2 {
		t.Fatalf("expected messages 0 and 2 once (1 abandoned), got %v", ids)
	}
}

func TestPublisher_Unroutable(t *testing.T) {
	ch, queueName := testQueue(t)
	mq := NewRabbitMQClient(log.NewConsoleLogger(log.LogLevelWarn), "guest:guest@"+testBrokerAddr, 100*time.Millisecond, "test-client")
	p := mq.NewPublisher("publisher", WithMandatory(true))
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	noQueue := queueName + ".missing"
	if err := p.Publish(ctx, Publication{Key: noQueue, Body: []byte("1")}); !errors.Is(err, ErrUnroutable) {
		t.Fatalf("mandatory Publish to missing queue: expected %v, got %v", ErrUnroutable, err)
	}
	if err := p.Publish(ctx, Publication{Key: noQueue, Body: []byte("2"), Mandatory: MandatoryOff}); err != nil {
		t.Fatalf("not mandatory Publish to missing queue: unexpected error %v", err)
	}
	if err := p.Publish(ctx, Publication{Key: queueName, Body: []byte("3")}); err != nil {
		t.Fatalf("mandatory Publish to existing queue: unexpected error %v", err)
	}

	ids := queueMessageIds(t, ch, queueName)
	if ids[3] != 1 || len(ids) != 1 {
		t.Fatalf("expected only message 3, got %v", ids)
	}
}

func TestPublisher_ChannelException(t *testing.T) {
	ch, queueName := testQueue(t)
	mq := NewRabbitMQClient(log.NewConsoleLogger(log.LogLevelWarn), "guest:guest@"+testBrokerAddr, 100*time.Millisecond, "test-client")
	p := mq.NewPublisher("publisher")
	defer func() { _ = p.Shutdown(context.Background()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := p.Publish(ctx, Publication{Exchange: queueName + ".missing.exchange", Body: []byte("1")})
	if !errors.Is(err, ErrChannelClosed) {
		t.Fatalf("Publish to missing exchange: expected %v, got %v", ErrChannelClosed, err)
	}
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.NotFound {
		t.Fatalf("Publish to missing exchange: expected amqp NOT_FOUND error, got %v", err)
	}

	if err := p.Publish(ctx, Publication{Key: queueName, Body: []byte("2")}); err != nil {
		t.Fatalf("Publish after channel exception: unexpected error %v", err)
	}

	ids := queueMessageIds(t, ch, queueName)
	if ids[2] != 1 || len(ids) != 1 {
		t.Fatalf("expected only message 2, got %v", ids)
	}
}

// Deprecated Publish: sending after ctx is done must not panic nor block
func TestRabbitMQClient_PublishAfterContextDone(t *testing.T) {
	mq := NewRabbitMQClient(log.NewConsoleLogger(log.LogLevelError), "127.0.0.1:1", 100*time.Millisecond, "test-client")

	ctx, cancel := context.WithCancel(context.Background())
	out := mq.Publish(ctx, "publisher")
	cancel()

	for i := range 3 {
		select {
		case out <- Publication{Key: "test", Body: []byte(strconv.Itoa(i))}:
		case <-time.After(10 * time.Second):
			t.Fatalf("send %d after ctx done blocked", i)
		}
	}
}
