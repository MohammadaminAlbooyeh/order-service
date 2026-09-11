# order-service

Order lifecycle service for the platform. Owns orders and orchestrates the order saga
over Kafka (inventory → fraud → payment) with compensation on failure.

## Responsibilities

- Create orders from `cart.checkout` (idempotent per `orderId`) or direct REST `POST /api/orders`.
- Orchestrate saga: `order.created` → `inventory.reserved` → `order.awaiting_payment` → `payment.succeeded` → `order.confirmed`.
- Compensate: release inventory reservations on fraud flag, payment failure, or manual/timeout cancel.
- Expire stale `PENDING` / `INVENTORY_RESERVED` / `AWAITING_PAYMENT` orders after TTL (default 15 min).
- Consume `inventory.reserved`, `inventory.reservation_failed`, `fraud.flagged`, `payment.succeeded`, `payment.failed`.

## Tech

Spring Boot 3.3 · Java 17 · PostgreSQL + Flyway · Redis · Spring Kafka · springdoc OpenAPI

## Running locally

```bash
# Everything (Postgres, Redis, Kafka, service):
docker compose up --build

# Or just the infra, then run the app from your IDE / CLI with the dev profile (H2, no Flyway):
docker compose up postgres redis kafka
mvn spring-boot:run -Dspring-boot.run.profiles=dev
```

Service listens on `:8083`.

- Swagger UI: http://localhost:8083/swagger-ui.html
- Health: http://localhost:8083/actuator/health

## Build

```bash
mvn verify
```

`com.platform:java-common-lib:0.1.0-SNAPSHOT` (shared events + topic names) must be in the
local Maven repo. Build it first from the platform monorepo:

```bash
mvn -f ../shared/java-common-lib/pom.xml install
```

## Configuration

| Env var | Default | Purpose |
| --- | --- | --- |
| `DB_URL` | `jdbc:postgresql://localhost:5432/order_db` | JDBC URL |
| `DB_USERNAME` / `DB_PASSWORD` | `platform` / `platform` | DB credentials |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis for rate limiting |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka bootstrap |
| `API_KEY` | *(empty)* | If set, every `/api/**` call must send `X-API-Key: <value>`. Empty = open. |
| `RATE_LIMIT_ENABLED` | `true` | Redis-backed fixed-window limiter on `/api/**` |
| `RATE_LIMIT_REQUESTS` / `RATE_LIMIT_WINDOW_MS` | `120` / `10000` | Limiter budget and window |
| `OUTBOX_POLL_INTERVAL_MS` / `OUTBOX_BATCH_SIZE` | `1000` / `100` | Outbox relay poll delay and batch size |
| `ORDER_EXPIRY_TTL_MINUTES` / `ORDER_EXPIRY_ENABLED` | `15` / `true` | Stale-order TTL and kill switch |

## Security & operability

- **Auth**: static `X-API-Key` shared secret, enforced only when `API_KEY` is set (service
  is expected to sit behind the gateway). `/actuator/health`, `/actuator/info`,
  `/actuator/prometheus` and Swagger stay public.
- **Rate limiting**: Redis-backed per-client-IP fixed-window limiter returns `429` past the
  budget; the budget is shared across instances. Fails open if Redis is unreachable.
- **Reliable events**: outbound Kafka events are written to a transactional `outbox_events`
  table in the business transaction and drained by a relay, so a broker outage delays
  delivery but never loses an event. Dead-lettered records land on `<topic>.DLT` and are
  logged + counted (`order.kafka.dlt`).
- **Metrics**: `order.orders{outcome=created|confirmed|cancelled|expired}` style counters
  plus `order.outbox.pending` gauge, exposed at `/actuator/prometheus`.
- **List endpoints** are paged: `GET /api/orders?userId=..&page=0&size=20` returns a
  `PageResponse` (`content`, `totalElements`, …). Max page size 100.

## HTTP API

| Method | Path | Description |
| --- | --- | --- |
| POST | `/api/orders` | Create order (`userId`, `items[]`) → `201` |
| GET | `/api/orders/{orderId}` | Get one order |
| GET | `/api/orders?userId=&page=&size=` | List user orders (paged) |
| POST | `/api/orders/{orderId}/cancel` | Cancel (`{"reason":".."}`) |

## Kafka

| Direction | Topic | Payload |
| --- | --- | --- |
| in | `cart.checkout` | `CartCheckoutEvent` (shared lib) |
| in | `inventory.reserved` | `InventoryReservedEvent` |
| in | `inventory.reservation_failed` | `{ orderId, reason }` |
| in | `fraud.flagged` | `{ orderId, reason }` |
| in | `payment.succeeded` / `payment.failed` | `{ orderId, transactionId, ... }` |
| out | `order.created` | `{ orderId, userId, items, totalAmount }` |
| out | `order.awaiting_payment` | `{ orderId, amount, userId }` |
| out | `order.confirmed` | `{ orderId, userId, items }` |
| out | `order.cancelled` | `{ orderId, reason }` |
| out | `inventory.reservation_cancel` | `{ orderId }` (compensation) |

Consuming is idempotent per `(eventId, consumerGroup)` in `processed_events`. Consumer retries are bounded
(3 attempts, 1s back-off); after that the record is routed to `<topic>.DLT`.
