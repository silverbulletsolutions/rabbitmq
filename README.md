# RabbitMQ Client (Go)

Klient RabbitMQ dla Go odporny na zerwania połączenia, zbudowany na `github.com/rabbitmq/amqp091-go`
i module logowania `github.com/silverbulletsolutions/log`.

Pakiet ukrywa zarządzanie połączeniem AMQP: konsument i publisher działają w tle, same nawiązują połączenie
ponownie po awarii, a publisher z potwierdzeniami (*publisher confirms*) zgłasza wołającemu wynik każdej publikacji.

---

## Główne cechy

- **Automatyczny reconnect** – konsument i publisher wznawiają zerwane połączenie i kanał po `reconnectTime` (z losowym rozrzutem ±25%).
- **Publikacja z potwierdzeniem** – `Publisher.Publish` zwraca `nil` dopiero po potwierdzeniu przyjęcia wiadomości przez brokera; wiadomości niepotwierdzone przed zerwaniem połączenia są wysyłane ponownie (*at-least-once*).
- **Błędy zwracane wołającemu** – brak trasy (`ErrUnroutable`), odrzucenie przez brokera (`ErrNacked`), zamknięcie kanału (`ErrChannelClosed`), blokada połączenia (`ErrConnectionBlocked`).
- **Graceful shutdown** – `Publisher.Shutdown(ctx)` w modelu `http.Server`: wysyła i czeka na potwierdzenie wszystkiego, co zostało przyjęte, a po upływie `ctx` kończy natychmiast.
- **Kanałowe API konsumenta** – wiadomości odbierane z `<-chan amqp.Delivery`.
- **Kolejki** – `UseExistingQueue` (istniejąca kolejka) i `UseTemporaryQueue` (deklaracja kolejki tymczasowej z powiązaniami).
- **Bezpieczne logowanie** – hasło z connection stringa jest w logach zamieniane na `*` (tyle gwiazdek, ile znaków ma hasło).

---

## Instalacja

```bash
go get github.com/silverbulletsolutions/rabbitmq
```

Wymagania: Go `1.26` lub nowszy.

---

## `RabbitMQClient`

Główny punkt wejścia. Przechowuje adres brokera, czas ponawiania połączenia i nazwę klienta; tworzy konsumentów i publisherów.

```go
client := rabbitmq.NewRabbitMQClient(
    logger,                       // log.Logger
    "guest:guest@localhost:5672", // connectionString
    1*time.Second,                // reconnectTime - czas oczekiwania przed ponownym połączeniem
    "my-service",                 // clientName - nazwa aplikacji/usługi
)
```

- `connectionString` może zaczynać się od `amqp://` lub `amqps://` (TLS); bez schematu dodawany jest `amqp://`.
- Odstęp między próbami połączenia to `reconnectTime` ±25% (rozrzut chroni brokera przed falą jednoczesnych połączeń po awarii). Oczekiwanie jest przerywane przez zamknięcie konsumenta / publishera.
- Każde połączenie używa heartbeatu AMQP co 10 s – martwe łącze (bez zamknięcia TCP) jest wykrywane najpóźniej po ok. 30 s.
- Nawiązanie połączenia (TCP, TLS, handshake AMQP) ma limit 30 s i jest przerywane przez zamknięcie konsumenta / publishera.
- Identyfikator sesji widoczny w logach i w polu `AppId` wiadomości ma postać `clientName-nazwa#numer`.

---

## Publikowanie – `Publisher`

```go
p := client.NewPublisher("order-publisher") // łączy się w tle
defer func() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := p.Shutdown(ctx); err != nil {
        logger.Error("Publisher shutdown: {_error}", log.Data{"_error": log.Err(err)})
    }
}()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

err := p.Publish(ctx, rabbitmq.Publication{
    Exchange:      "orders.exchange",
    Key:           "order.created",
    ContentType:   "application/json",
    CorrelationId: "req-12345",
    Body:          []byte(`{"orderId": 42, "amount": 99.99}`),
})
switch {
case err == nil:
    // broker potwierdził przyjęcie wiadomości
case errors.Is(err, rabbitmq.ErrUnroutable):
    // wiadomość mandatory nie trafiła do żadnej kolejki
case errors.Is(err, context.DeadlineExceeded):
    // wołający zrezygnował (patrz niżej - wiadomość mogła zostać dostarczona)
default:
    // inne błędy
}
```

`Publisher` jest bezpieczny do współbieżnego użycia. Publisher, który nie został zamknięty przez `Shutdown`,
utrzymuje połączenie i goroutine do końca działania programu.

### Opcje

| opcja | domyślnie | znaczenie |
|---|---|---|
| `WithoutConfirms()` | potwierdzenia włączone | `Publish` kończy się po zapisaniu wiadomości do socketu; przy zerwaniu połączenia wiadomość może zginąć bez błędu |
| `WithMandatory(bool)` | `false` | domyślna flaga `mandatory`: wiadomość, której nie da się skierować do żadnej kolejki, kończy się `ErrUnroutable` zamiast zniknąć po cichu |
| `WithPersistent(bool)` | `false` | domyślny tryb dostarczenia: wiadomość trwała (`true`) przetrwa restart brokera, jeśli trafi do trwałej kolejki – broker potwierdza ją dopiero po zapisie na dysk |
| `WithMaxInFlight(n)` | `256` | maksymalna liczba wysłanych, a jeszcze niepotwierdzonych wiadomości; po osiągnięciu limitu kolejne czekają |

Flagę `mandatory` można zmienić dla pojedynczej wiadomości polem `Publication.Mandatory`
(`MandatoryOn` / `MandatoryOff`; zero – `MandatoryDefault` – oznacza ustawienie publishera).
Analogicznie tryb dostarczenia zmienia pole `Publication.DeliveryMode` (`DeliveryModePersistent` / `DeliveryModeTransient`).

### Trwałość wiadomości

Wiadomość przetrwa restart brokera tylko wtedy, gdy jest trwała (`WithPersistent(true)` lub `DeliveryModePersistent`)
**i** trafia do trwałej kolejki (`durable`, np. kolejka quorum). Dla takiej wiadomości potwierdzenie przychodzi
po zapisie na dysk (kolejka quorum: po replikacji na większość węzłów), więc `Publish` zwracające `nil` oznacza,
że restart brokera jej nie usunie. Trwała wiadomość w nietrwałej kolejce ginie razem z kolejką.
Zapis na dysk wydłuża czas potwierdzenia.

### Wynik `Publish`

| wynik | znaczenie |
|---|---|
| `nil` | broker potwierdził wiadomość (z `WithoutConfirms()`: wiadomość zapisana do socketu) |
| `ErrUnroutable` | wiadomość `mandatory` nie trafiła do żadnej kolejki (wymaga potwierdzeń; bez nich zwrot jest tylko logowany) |
| `ErrNacked` | broker odrzucił wiadomość |
| `ErrChannelClosed` | broker zamknął kanał przed potwierdzeniem (np. nieistniejący exchange); oryginalny `*amqp.Error` dostępny przez `errors.As` |
| błąd `ctx` | wołający zrezygnował; jeśli połączenie jest zablokowane przez brokera, błąd spełnia też `errors.Is(err, ErrConnectionBlocked)` i zawiera powód blokady |
| `ErrPublisherClosed` | publisher jest zamknięty / zamykany, albo minął termin `Shutdown` zanim wiadomość została wysłana lub potwierdzona |

### Zachowanie przy awariach

- **Brak połączenia** – `Publish` czeka na ponowne połączenie (ograniczone przez `ctx` wołającego).
- **Zerwanie połączenia** (restart brokera, awaria sieci, martwe łącze wykryte przez heartbeat) – wiadomości wysłane, ale niepotwierdzone, są po ponownym połączeniu wysyłane jeszcze raz, w pierwotnej kolejności i przed nowymi. **Konsumenci mogą więc otrzymać duplikaty** – przetwarzanie powinno być idempotentne (np. po `CorrelationId`).
- **Zamknięcie kanału przez brokera** (błąd kanału, np. 404 dla nieistniejącego exchange) – niepotwierdzone wiadomości dostają `ErrChannelClosed` i nie są ponawiane (jedna z nich spowodowała błąd); publisher otwiera nowy kanał i działa dalej.
- **Blokada połączenia przez brokera** (alarm pamięci / dysku) – publisher wstrzymuje wysyłkę do czasu zdjęcia blokady; `Publish` czeka do upływu swojego `ctx`.
- **Rezygnacja wołającego** – wiadomość, której `ctx` się zakończył, nie jest wysyłana (także ponownie po reconnect). Jeśli została już wysłana, mogła zostać dostarczona.

### Zamykanie – `Shutdown(ctx)`

1. Publisher przestaje przyjmować nowe wiadomości (`Publish` zwraca `ErrPublisherClosed`).
2. Wysyła wszystkie przyjęte wiadomości i czeka na ich potwierdzenie – w razie potrzeby łącząc się ponownie.
3. Zamyka kanał (co czeka na przetworzenie przez brokera wszystkiego, co zostało wysłane) i połączenie, po czym zwraca `nil`.

Jeśli `ctx` zakończy się wcześniej, niewysłane / niepotwierdzone wiadomości dostają `ErrPublisherClosed`,
połączenie jest zamykane natychmiast, a `Shutdown` zwraca błąd `ctx`. `Shutdown` można wywołać wielokrotnie;
`Done()` zwraca kanał zamykany po zakończeniu pracy publishera.

### Uwagi

- Domyślnie wiadomości są wysyłane jako nietrwałe (`Transient`) – nie przetrwają restartu brokera nawet w trwałej kolejce.
- Wiadomości z `mandatory` (przy włączonych potwierdzeniach) dostają nagłówek `x-publish-id`, służący do powiązania zwrotu z wywołaniem `Publish`. Konsumenci go widzą.
- Kolejne wywołania `Publish` z jednej goroutine są ograniczone opóźnieniem potwierdzenia brokera (jedno potwierdzenie na wywołanie). Dla dużej przepustowości publikuj współbieżnie – do `WithMaxInFlight` wiadomości jest w drodze jednocześnie.

### `RabbitMQClient.Publish` (przestarzałe)

```go
ch := client.Publish(ctx, "order-publisher") // Deprecated: użyj NewPublisher
ch <- rabbitmq.Publication{ /* ... */ }
```

Zachowane dla zgodności wstecz; działa na `Publisher` z domyślnymi opcjami:

- wysłanie do kanału czeka na przyjęcie wiadomości, ale nie na potwierdzenie; błędy publikacji są tylko logowane;
- po zakończeniu `ctx` wiadomości przyjęte wcześniej są wysyłane w ramach 5-sekundowego `Shutdown`;
- kanał nigdy nie jest zamykany (wysyłający dostawaliby panic) – wiadomości wysłane po zakończeniu `ctx` są odrzucane i logowane.

---

## Migracja z `RabbitMQClient.Publish` na `Publisher`

### Zmiany zachowania bez zmiany kodu

Kod używający `client.Publish(ctx, name)` kompiluje się i działa dalej, ale zachowuje się inaczej niż wcześniej:

| | wcześniej | teraz |
|---|---|---|
| wysłanie do kanału po zakończeniu `ctx` | panic (kanał był zamykany) | wiadomość odrzucona i zalogowana |
| zerwanie połączenia | ponawiana tylko wiadomość, której zapis do socketu się nie powiódł; wiadomości zapisane w martwe łącze ginęły bez śladu | ponawiane wszystkie wiadomości niepotwierdzone przez brokera (możliwe duplikaty) |
| zakończenie `ctx` | połączenie zamykane od razu | przyjęte wiadomości są wysyłane i potwierdzane w ramach 5-sekundowego zamykania |
| nieistniejący exchange | broker zamykał kanał; wiadomości wysłane, zanim klient to wykrył, ginęły bez śladu | błąd `ErrChannelClosed` logowany dla każdej z nich |
| błędy publikacji | niewidoczne dla wołającego | nadal niewidoczne – tylko logowane |

### Krok 1 – jeden publisher na czas życia aplikacji

Utwórz publisher raz przy starcie (nie na każdy request) i przekaż go tam, gdzie wcześniej przekazywany był kanał:

```go
// przed
pubCh := client.Publish(ctx, "order-publisher")

// po
pub := client.NewPublisher("order-publisher") // opcjonalnie: WithMandatory(true), WithPersistent(true)
```

### Krok 2 – `Publish` zamiast wysłania do kanału

```go
// przed
pubCh <- rabbitmq.Publication{Exchange: "orders", Key: "order.created", Body: body}

// po
if err := pub.Publish(ctx, rabbitmq.Publication{Exchange: "orders", Key: "order.created", Body: body}); err != nil {
    return fmt.Errorf("publish order.created: %w", err)
}
```

- `Publish` czeka na potwierdzenie brokera – błąd można zwrócić klientowi, ponowić albo zalogować.
- `ctx` ogranicza czas oczekiwania (także na ponowne połączenie). W handlerze HTTP `r.Context()` oznacza,
  że rozłączenie klienta przerywa publikację; jeśli wiadomość ma wyjść niezależnie od klienta, użyj
  `context.WithTimeout(context.WithoutCancel(r.Context()), ...)`.
- Kolejne `Publish` z jednej goroutine czekają każde na swoje potwierdzenie. Jeśli kod wysyłał w pętli wiele
  wiadomości, publikuj je współbieżnie (np. `errgroup`), żeby nie płacić opóźnienia potwierdzenia za każdą osobno.
- Jeśli wynik naprawdę nie jest potrzebny (*fire-and-forget*), wywołaj `Publish` w osobnej goroutine i zaloguj błąd –
  ale wtedy goroutine muszą zakończyć się przed `Shutdown` (patrz krok 3).

### Krok 3 – zamykanie

Zamiast anulowania `ctx` przekazanego do `client.Publish` wywołaj `Shutdown` – **dopiero gdy nikt już nie publikuje**.
Wywołanie `Publish` po (lub w trakcie) `Shutdown` zwraca `ErrPublisherClosed`, nawet jeśli zaczęło się wcześniej,
ale wiadomość nie została jeszcze przyjęta.

W serwerze HTTP kolejność jest następująca:

```go
// 1. przestań przyjmować requesty i poczekaj na trwające
if err := srv.Shutdown(srvCtx); err != nil {
    _ = srv.Close() // termin minął - zerwij połączenia (anuluje r.Context())
    // poczekaj chwilę na handlery, które jeszcze publikują (np. własny sync.WaitGroup)
}

// 2. wszystkie Publish się zakończyły - zamknij publisher
if err := pub.Shutdown(pubCtx); err != nil {
    logger.Error("Publisher shutdown: {_error}", log.Data{"_error": log.Err(err)})
}
```

`srv.Shutdown` nie czeka na goroutine uruchomione z handlerów ani na połączenia przejęte przez `Hijack` –
jeśli publikują, trzeba poczekać na nie samodzielnie przed `pub.Shutdown`.

---

## Konsumpcja – `Consume`

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

deliveries := client.Consume(
    ctx,
    rabbitmq.UseExistingQueue("orders.incoming"),
    "order-consumer", // nazwa konsumenta (consumer tag: clientName-nazwa#numer)
    20,               // prefetchCount (0 - bez limitu)
    false,            // autoAck
    nil,              // argumenty basic.consume
)

for msg := range deliveries {
    // przetwarzanie...
    _ = msg.Ack(false)
}
```

- Kanał `deliveries` jest zamykany po zakończeniu `ctx`.
- Po zerwaniu połączenia (lub błędzie deklaracji kolejki / `basic.consume`) konsument łączy się ponownie, ponownie inicjalizuje kolejkę i wznawia odbiór.
- Wiadomości odebrane przed zerwaniem połączenia nie mogą już zostać potwierdzone (`Ack` zwraca błąd) – broker dostarczy je ponownie z `Redelivered: true`.
- Kanał `deliveries` jest niebuforowany – wolne przetwarzanie wstrzymuje odbiór (liczbę wiadomości po stronie klienta ogranicza `prefetchCount`).

### Kolejki (`RabbitMQQueue`)

Kolejka jest inicjalizowana przy każdym nawiązaniu połączenia:

- **`UseExistingQueue(name)`** – sprawdza, że kolejka istnieje (`QueueDeclarePassive`); jeśli nie, konsument ponawia próbę po `reconnectTime`.
- **`UseTemporaryQueue(name)`** – deklaruje nietrwałą kolejkę (`durable: false`, `autoDelete: true`, domyślnie `exclusive: true`):
  - `.NoExclusive()` – wyłącza wyłączność (patrz uwaga niżej),
  - `.AddArg(key, value)` – argument kolejki (np. `x-message-ttl`, `x-max-length`),
  - `.Bind(exchange, args)` – powiązanie z exchange,
  - `.BindWithKey(exchange, routingKey, args)` – powiązanie z kluczem routingu.

```go
q := rabbitmq.UseTemporaryQueue("notifications-worker").
    BindWithKey("events.topic", "user.created", nil).
    BindWithKey("events.topic", "user.updated", nil)

deliveries := client.Consume(ctx, q, "notify-consumer", 10, true, nil)
```

> **RabbitMQ 4.x:** nietrwałe kolejki niewyłączne są wycofane (*deprecated feature* `transient_nonexcl_queues`)
> i domyślnie zabronione. `UseTemporaryQueue(...).NoExclusive()` kończy się wtedy błędem deklaracji,
> a konsument ponawia próbę w nieskończoność.

---

## Testy

Testy integracyjne wymagają brokera RabbitMQ na `127.0.0.1:5672` z użytkownikiem `guest:guest`:

```bash
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:4
go test -count=1 -v .
```

Testy zerwania połączenia (`PublisherReconnect_test.go`) łączą się z brokerem przez lokalny proxy TCP, który symuluje
zerwanie połączeń, niedostępność brokera i martwe łącze. Gdy broker jest niedostępny, testy te są pomijane.
