package rabbitmq

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/silverbulletsolutions/log"
)

// testSharedQueue declares a durable queue - it can be consumed by other connections (unlike exclusive queues)
func testSharedQueue(t *testing.T) (*amqp.Channel, string) {
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
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = ch.QueueDelete(queueName, false, false, false) })

	return ch, queueName
}

func publishToQueue(t *testing.T, ch *amqp.Channel, queueName string, ids ...int) {
	for _, id := range ids {
		err := ch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
			Body:          []byte(strconv.Itoa(id)),
			CorrelationId: strconv.Itoa(id),
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// readyMessages returns number of messages in the queue waiting for delivery (not counting unacknowledged ones)
func readyMessages(t *testing.T, ch *amqp.Channel, queueName string) int {
	q, err := ch.QueueDeclarePassive(queueName, true, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	return q.Messages
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func newTestClient() *RabbitMQClient {
	return NewRabbitMQClient(log.NewConsoleLogger(log.LogLevelWarn), "guest:guest@"+testBrokerAddr, 100*time.Millisecond, "test-client")
}

// processedIds counts handled messages by id (body)
type processedIds struct {
	mu  sync.Mutex
	ids map[int]int
}

func (p *processedIds) add(d *Delivery) {
	id, _ := strconv.Atoi(string(d.Body))
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ids == nil {
		p.ids = make(map[int]int)
	}
	p.ids[id]++
}

func (p *processedIds) distinct() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.ids)
}

func (p *processedIds) count(id int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ids[id]
}

func shutdownConsumer(t *testing.T, c *Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: unexpected error %v", err)
	}
}

func TestConsumer_Configuration(t *testing.T) {
	mq := NewRabbitMQClient(log.NewConsoleLogger(log.LogLevelError), "127.0.0.1:1", 100*time.Millisecond, "test-client")
	h := HandlerFunc(func(ctx context.Context, d *Delivery) error { return nil })

	if _, err := mq.NewConsumer("consumer", UseExistingQueue("q"), nil); err == nil {
		t.Error("nil handler: expected error")
	}
	if _, err := mq.NewConsumer("consumer", nil, h); err == nil {
		t.Error("nil queue: expected error")
	}
	if _, err := mq.NewConsumer("consumer", UseExistingQueue("q"), h, WithConcurrency(4), WithPrefetch(2)); err == nil {
		t.Error("prefetch lower than concurrency: expected error")
	}
	if _, err := mq.NewConsumer("consumer", UseExistingQueue("q"), h, WithConcurrency(4), WithPrefetch(2), WithAutoAck()); err != nil {
		t.Errorf("prefetch with auto ack: unexpected error %v", err)
	}
}

func TestConsumer_StartShutdownWithoutBroker(t *testing.T) {
	mq := NewRabbitMQClient(log.NewConsoleLogger(log.LogLevelError), "127.0.0.1:1", 100*time.Millisecond, "test-client")
	h := HandlerFunc(func(ctx context.Context, d *Delivery) error { return nil })

	// Shutdown before Start
	c, err := mq.NewConsumer("consumer", UseExistingQueue("q"), h)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Start: unexpected error %v", err)
	}
	<-c.Done()
	if err := c.Start(); !errors.Is(err, ErrConsumerClosed) {
		t.Fatalf("Start after Shutdown: expected %v, got %v", ErrConsumerClosed, err)
	}

	// Start (twice) while broker unavailable, Shutdown interrupts reconnecting
	c, err = mq.NewConsumer("consumer", UseExistingQueue("q"), h)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("Start: unexpected error %v", err)
	}
	if err := c.Start(); err != nil {
		t.Fatalf("second Start: unexpected error %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: unexpected error %v", err)
	}
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown: unexpected error %v", err)
	}
}

func TestConsumer_Concurrency(t *testing.T) {
	ch, queueName := testSharedQueue(t)
	const messages = 20
	ids := make([]int, messages)
	for i := range ids {
		ids[i] = i
	}
	publishToQueue(t, ch, queueName, ids...)

	var processed processedIds
	var running, maxRunning atomic.Int32
	h := HandlerFunc(func(ctx context.Context, d *Delivery) error {
		n := running.Add(1)
		for m := maxRunning.Load(); n > m && !maxRunning.CompareAndSwap(m, n); m = maxRunning.Load() {
		}
		time.Sleep(100 * time.Millisecond)
		running.Add(-1)
		processed.add(d)
		return nil
	})

	c, err := newTestClient().NewConsumer("consumer", UseExistingQueue(queueName), h, WithConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "all messages processed", 10*time.Second, func() bool { return processed.distinct() == messages })
	shutdownConsumer(t, c)

	if m := maxRunning.Load(); m != 4 {
		t.Errorf("expected 4 handlers running concurrently, max was %d", m)
	}
	if n := readyMessages(t, ch, queueName); n != 0 {
		t.Errorf("expected empty queue, %d messages left", n)
	}
}

// Shutdown cancels the subscription and waits for the running handler; its message is acknowledged,
// messages not delivered yet stay in the queue
func TestConsumer_ShutdownWaitsForHandler(t *testing.T) {
	ch, queueName := testSharedQueue(t)
	publishToQueue(t, ch, queueName, 0, 1, 2)

	var processed processedIds
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	h := HandlerFunc(func(ctx context.Context, d *Delivery) error {
		if string(d.Body) == "0" {
			started <- struct{}{}
			<-release
		}
		processed.add(d)
		return nil
	})

	c, err := newTestClient().NewConsumer("consumer", UseExistingQueue(queueName), h)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, "handler start", async(func() error { <-started; return nil }), 10*time.Second); err != nil {
		t.Fatal(err)
	}

	shutdown := async(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return c.Shutdown(ctx)
	})
	select {
	case err := <-shutdown:
		t.Fatalf("Shutdown returned (%v) before handler finished", err)
	case <-time.After(300 * time.Millisecond):
	}

	close(release)
	if err := waitResult(t, "Shutdown", shutdown, 10*time.Second); err != nil {
		t.Fatalf("Shutdown: unexpected error %v", err)
	}

	if processed.count(0) != 1 || processed.distinct() != 1 {
		t.Errorf("expected only message 0 processed, got %v", processed.ids)
	}
	if n := readyMessages(t, ch, queueName); n != 2 {
		t.Errorf("expected 2 messages left in the queue, got %d", n)
	}
}

// Shutdown deadline expires - handler ctx is cancelled, its message returns to the queue
func TestConsumer_ShutdownDeadline(t *testing.T) {
	ch, queueName := testSharedQueue(t)
	publishToQueue(t, ch, queueName, 0)

	started := make(chan struct{}, 1)
	h := HandlerFunc(func(ctx context.Context, d *Delivery) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	})

	c, err := newTestClient().NewConsumer("consumer", UseExistingQueue(queueName), h)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, "handler start", async(func() error { <-started; return nil }), 10*time.Second); err != nil {
		t.Fatal(err)
	}

	const deadline = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	start := time.Now()
	if err := c.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown: expected %v, got %v", context.DeadlineExceeded, err)
	}
	if d := time.Since(start); d > deadline+time.Second {
		t.Fatalf("Shutdown took %v (deadline %v)", d, deadline)
	}

	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done not closed after handler returned")
	}

	waitFor(t, "message returned to the queue", 5*time.Second, func() bool { return readyMessages(t, ch, queueName) == 1 })
}

func TestConsumer_Acknowledgement(t *testing.T) {
	ch, queueName := testSharedQueue(t)
	// 0 - handler error (rejected), 1 - explicit Nack with requeue (processed when redelivered),
	// 2 - double Ack (second returns ErrAlreadyAcknowledged), 3 - panic (requeued after delay, processed when redelivered)
	publishToQueue(t, ch, queueName, 0, 1, 2, 3)

	const panicDelay = 300 * time.Millisecond
	var processed processedIds
	var doubleAckErr atomic.Pointer[error]
	var panicAt, redeliveredAt atomic.Pointer[time.Time]
	h := HandlerFunc(func(ctx context.Context, d *Delivery) error {
		processed.add(d)
		switch string(d.Body) {
		case "0":
			return errors.New("processing failed")
		case "1":
			if !d.Redelivered {
				return d.Nack(true)
			}
		case "2":
			_ = d.Ack()
			err := d.Ack()
			doubleAckErr.Store(&err)
		case "3":
			now := time.Now()
			if !d.Redelivered {
				panicAt.Store(&now)
				panic("handler panic")
			}
			redeliveredAt.Store(&now)
		}
		return nil
	})

	c, err := newTestClient().NewConsumer("consumer", UseExistingQueue(queueName), h, WithPanicRequeueDelay(panicDelay))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "all messages processed", 10*time.Second, func() bool {
		return processed.distinct() == 4 && processed.count(1) == 2 && processed.count(3) == 2
	})
	time.Sleep(300 * time.Millisecond) // rejected messages would be redelivered by now
	shutdownConsumer(t, c)

	for _, id := range []int{0, 2} {
		if n := processed.count(id); n != 1 {
			t.Errorf("message %d: expected to be processed once, got %d", id, n)
		}
	}
	if p := doubleAckErr.Load(); p == nil || !errors.Is(*p, ErrAlreadyAcknowledged) {
		t.Errorf("double Ack: expected %v", ErrAlreadyAcknowledged)
	}
	if d := redeliveredAt.Load().Sub(*panicAt.Load()); d < panicDelay {
		t.Errorf("message 3 redelivered %v after panic, expected at least %v", d, panicDelay)
	}
	if n := readyMessages(t, ch, queueName); n != 0 {
		t.Errorf("expected empty queue, %d messages left", n)
	}
}

// Shutdown interrupts panic requeue delay - message is requeued immediately
func TestConsumer_ShutdownInterruptsPanicDelay(t *testing.T) {
	ch, queueName := testSharedQueue(t)
	publishToQueue(t, ch, queueName, 0)

	panicked := make(chan struct{}, 1)
	h := HandlerFunc(func(ctx context.Context, d *Delivery) error {
		panicked <- struct{}{}
		panic("handler panic")
	})

	c, err := newTestClient().NewConsumer("consumer", UseExistingQueue(queueName), h, WithPanicRequeueDelay(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, "handler panic", async(func() error { <-panicked; return nil }), 10*time.Second); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	shutdownConsumer(t, c)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Shutdown took %v - panic requeue delay not interrupted", d)
	}
	if n := readyMessages(t, ch, queueName); n != 1 {
		t.Errorf("expected message requeued, %d messages in the queue", n)
	}
}

// Deprecated Consume: nobody reads the channel - cancelling ctx must not leave the goroutine blocked on send
func TestRabbitMQClient_ConsumeCancelWithoutReader(t *testing.T) {
	ch, queueName := testSharedQueue(t)
	publishToQueue(t, ch, queueName, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deliveries := newTestClient().Consume(ctx, UseExistingQueue(queueName), "consumer", 1, false, nil)

	time.Sleep(500 * time.Millisecond) // message received, send to the channel blocked
	cancel()
	time.Sleep(300 * time.Millisecond)

	select {
	case m, ok := <-deliveries:
		if ok {
			t.Fatalf("expected closed channel, got message %s", m.Body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel not closed after ctx cancel - goroutine blocked on send")
	}

	waitFor(t, "message returned to the queue", 5*time.Second, func() bool { return readyMessages(t, ch, queueName) == 1 })
}

// Deprecated Consume: queue deleted (subscription cancelled by broker) and created again - consumption resumes
func TestRabbitMQClient_ConsumeAfterQueueRecreated(t *testing.T) {
	ch, queueName := testSharedQueue(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deliveries := newTestClient().Consume(ctx, UseExistingQueue(queueName), "consumer", 1, false, nil)

	publishToQueue(t, ch, queueName, 0)
	select {
	case m := <-deliveries:
		_ = m.Ack(false)
	case <-time.After(5 * time.Second):
		t.Fatal("message 0 not received")
	}

	if _, err := ch.QueueDelete(queueName, false, false, false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	publishToQueue(t, ch, queueName, 1)

	select {
	case m := <-deliveries:
		if string(m.Body) != "1" {
			t.Fatalf("expected message 1, got %s", m.Body)
		}
		_ = m.Ack(false)
	case <-time.After(10 * time.Second):
		t.Fatal("message 1 not received after queue was recreated")
	}
}

// Connection dropped while consuming - consumer reconnects, every message is processed (duplicates allowed)
func TestConsumer_Reconnect(t *testing.T) {
	ch, queueName := testSharedQueue(t)
	const messages = 50
	ids := make([]int, messages)
	for i := range ids {
		ids[i] = i
	}
	publishToQueue(t, ch, queueName, ids...)

	mq, proxy := newProxiedClient(t)
	var processed processedIds
	h := HandlerFunc(func(ctx context.Context, d *Delivery) error {
		time.Sleep(20 * time.Millisecond)
		processed.add(d)
		return nil
	})

	c, err := mq.NewConsumer("consumer", UseExistingQueue(queueName), h, WithConcurrency(4))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "some messages processed", 10*time.Second, func() bool { return processed.distinct() >= 10 })
	proxy.Drop()

	waitFor(t, "all messages processed", 10*time.Second, func() bool { return processed.distinct() == messages })
	shutdownConsumer(t, c)

	if n := readyMessages(t, ch, queueName); n != 0 {
		t.Errorf("expected empty queue, %d messages left", n)
	}
}
