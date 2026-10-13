# RabbitMQ Client (Go)

Wydajny, odporny na awarie i prosty w użyciu klient RabbitMQ dla języka Go, zbudowany w oparciu
o bibliotekę `github.com/rabbitmq/amqp091-go` oraz moduł logowania `github.com/silverbulletsolutions/log`.

Pakiet dostarcza abstrakcję wysokiego poziomu ułatwiającą konsumpcję oraz publikację wiadomości
z wbudowanym mechanizmem automatycznego wznawiania połączenia (*reconnect*) i ponawiania nieudanych publikacji (*retry*).

---

## Główne cechy

- **Automatyczny reconnect**: Konsument i producent działają w pętli w tle, automatycznie wznawiając zerwane połączenia AMQP i kanały po zadanym czasie (`reconnectTime`).
- **Gwarancja dostarczenia przy publikacji**: W przypadku utraty połączenia podczas wysyłki,
- niewysłana wiadomość jest zachowywana i ponawiana natychmiast po przywróceniu łączności.
- **Kanałowe API Go**: Wykorzystanie natywnych kanałów Go (`<-chan amqp.Delivery`, `chan<- Publication`) do asynchronicznego przesyłania i odbierania wiadomości.
- **Elastyczne zarządzanie kolejkami**:
  - `UseExistingQueue` – pasywne podłączenie do istniejącej już kolejki na serwerze.
  - `TemporaryQueue` (`UseTemporaryQueue`) – deklaracja i konfiguracja kolejek tymczasowych/ekskluzywnych z fluent builderem powiązań (*bindings*) i argumentów.
- **Pełna kontrola kontekstem**: Wsparcie dla `context.Context` do bezpiecznego zatrzymywania procesów konsumpcji i publikacji (*graceful shutdown*).
- **Strukturalne logowanie**: Integracja z loggerem zoptymalizowanym pod kątem czytelności i śledzenia zdarzeń.

---

## Instalacja

```bash
go get github.com/silverbulletsolutions/rabbitmq
```

Wymagania: Go w wersji `1.26` lub nowszej.

---

## Architektura i kluczowe komponenty

### 1. `RabbitMQClient`
Główny punkt wejścia do biblioteki. Przechowuje konfigurację połączenia, interwał ponawiania prób oraz identyfikator klienta.

```go
client := rabbitmq.NewRabbitMQClient(
    logger,                // log.Logger
    "localhost:5672",      // connectionString (prefiks amqp:// jest dodawany automatycznie, jeśli brak)
    1 * time.Second,       // reconnectTime - czas oczekiwania przed próbą ponownego połączenia
    "my-service",          // clientName - nazwa aplikacji/usługi
)
```

### 2. Obsługa kolejek (`RabbitMQQueue`)
Interfejs pozwalający na zainicjalizowanie kolejki przy każdym nawiązaniu nowego połączenia:

- **`UseExistingQueue(name)`**: Weryfikuje istnienie kolejki (`QueueDeclarePassive`).
- **`UseTemporaryQueue(name)`**: Tworzy kolejkę (domyślnie `exclusive: true`, `autoDelete: true`).
  - `.NoExclusive()` – wyłącza tryb wyłączności.
  - `.AddArg(key, value)` – dodaje niestandardowe argumenty kolejki (np. TTL, max-length).
  - `.Bind(exchangeName, args)` – tworzy powiązanie z danym exchange.
  - `.BindWithKey(exchangeName, routingKey, args)` – tworzy powiązanie z exchange przy użyciu klucza routingu.

---

## Przykłady użycia

### Konsumpcja wiadomości z istniejącej kolejki

```go
package main

import (
    "context"
    "time"

    "git.silverbullet.pl/golang-common-modules/log"
    "github.com/silverbulletsolutions/rabbitmq"
)

func main() {
    logger := log.NewConsoleLogger(log.LogLevelInfo)
    mq := rabbitmq.NewRabbitMQClient(logger, "localhost:5672", 2*time.Second, "order-service")

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Odbieranie z istniejącej kolejki:
    // prefetchCount = 20, autoAck = false, args = nil
    deliveries := mq.Consume(
        ctx,
        rabbitmq.UseExistingQueue("orders.incoming"),
        "order-consumer",
        20,
        false,
        nil,
    )

    for msg := range deliveries {
        logger.Info("Odebrano wiadomość: " + string(msg.Body), log.Data{})
        _ = msg.Ack(false)
    }
}
```

### Konsumpcja z kolejki tymczasowej z routingiem

```go
tempQueue := rabbitmq.UseTemporaryQueue("notifications-worker").
    NoExclusive().
    BindWithKey("events.topic", "user.created", nil).
    BindWithKey("events.topic", "user.updated", nil)

deliveries := mq.Consume(ctx, tempQueue, "notify-consumer", 10, true, nil)

for msg := range deliveries {
    // Przetwarzanie zdarzenia...
}
```

### Publikowanie wiadomości

```go
package main

import (
    "context"
    "time"

    "git.silverbullet.pl/golang-common-modules/log"
    "github.com/silverbulletsolutions/rabbitmq"
)

func main() {
    logger := log.NewConsoleLogger(log.LogLevelInfo)
    mq := rabbitmq.NewRabbitMQClient(logger, "amqp://localhost:5672", 1*time.Second, "order-service")

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    publisherChannel := mq.Publish(ctx, "order-publisher")

    // Wysłanie wiadomości
    publisherChannel <- rabbitmq.Publication{
        Exchange:        "orders.exchange",
        Key:             "order.created",
        ContentType:     "application/json",
        CorrelationId:   "req-12345",
        Body:            []byte(`{"orderId": 42, "amount": 99.99}`),
    }

    // Zamknięcie kontekstu kończy działanie pętli publikującej
    time.Sleep(100 * time.Millisecond)
}
```

---

## Odporność na błędy (Fault Tolerance)

1. **Utrata połączenia w trakcie konsumpcji**: Klient wykrywa błąd na kanale lub połączeniu, zwalnia stare zasoby i podejmuje próbę ponownego połączenia co określony czas `reconnectTime`. Po ponownym połączeniu kolejka jest ponownie deklarowana/weryfikowana, a odbiór wiadomości wznawiany bez konieczności restartu aplikacji.
2. **Utrata połączenia w trakcie publikacji**: Ostatnia wiadomość, której wysyłka się nie powiodła, jest buforowana w `publicationRetry` i wysyłana w pierwszej kolejności zaraz po wznowieniu sesji AMQP.
3. **Heartbeat**: Połączenia domyślnie używają 10-sekundowego interwału Heartbeat AMQP do szybkiego wykrywania zerwanych połączeń sieciowych.
