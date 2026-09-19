# GoHub Manager Architect Challenge Submission

## Overview

This submission redesigns GoHub's booking path and implements the Seat Inventory Service. The core decision is a hybrid saga: Booking owns the critical payment-and-seat state machine, while Notification and Audit consume events independently. 

Task 2 is a runnable Python 3.12/FastAPI service backed by PostgreSQL, Redis, and Kafka. 

Task 3 is an ordered incident runbook grounded in every supplied metric.

## Architecture summary

- Booking Service durably orchestrates hold, asynchronous payment, confirmation, and compensation. It never waits synchronously for the payment provider.
- Seat Inventory exclusively owns availability. PostgreSQL is authoritative, with `SELECT FOR UPDATE` on each seat; Redis is an expiring performance mirror, not a lock or source of truth.
- Each service owns its own database. Transactional outboxes and idempotent consumers bridge local transactions to Kafka's at-least-once delivery.
- The hold is exactly 480 seconds by default. A `SKIP LOCKED` expiry worker releases overdue holds and every command checks the database expiry under lock, preventing a late-confirm race.
- Notification reacts to `booking.confirmed`, persists attempts, and uses per-operator timeout, signed webhooks, retry/backoff, and alerts to meet the 30-second target without changing booking success.
- Every Seat transition creates a local audit record and outbox event in the same transaction. Audit Service independently builds the cross-service immutable history.

The full diagram is in [task1/diagram.mmd](task1/diagram.mmd) and the decision record is in [task1/ADR-001-saga-pattern.md](task1/ADR-001-saga-pattern.md).

## Repository layout

```text
README.md
SUBMISSION-CHECKLIST.md
task1/
  ADR-001-saga-pattern.md
  diagram.mmd
  diagram.png
task2/
  app/
  migrations/
  tests/
  .env.example
  Dockerfile
  docker-compose.yml
  requirements.txt
task3/
  incident-response.md
```

## Assumptions

- A `seat_id` uniquely identifies one physical seat occurrence on one trip. If upstream identifiers only identify a vehicle seat, the production key should be `(operator_id, trip_id, seat_number)`.
- Authentication is handled by an API gateway. `user_id` is shown in the assessment API body for clarity; production code would derive it from a verified token and reject a mismatch.
- Payment authorization can be voided/refunded and the provider accepts an idempotency key. An unknown provider result must be reconciled, never blindly retried.
- Kafka is at least once and may redeliver. Consumers deduplicate by event ID and validate state-machine transitions.
- PostgreSQL and Redis use managed production deployments with TLS, authentication, backups, alerts, and multi-zone failover. Compose credentials are local-only.
- The requested eight-minute hold is within the assessment's five-to-ten-minute range.

## Run instructions

Requirements: Docker Engine with Compose v2 and ports `8000` available. From `task2/`:

```bash
cp .env.example .env
docker compose down -v --remove-orphans
docker compose build --no-cache
docker compose up -d
docker compose ps
curl -fsS http://localhost:8000/health
curl -fsS http://localhost:8000/metrics | grep seat_hold
```

Compose waits for PostgreSQL, Redis, Kafka, the migration job, and API health. It seeds `demo-seat-1` and `demo-seat-2` on `demo-trip-1`. API documentation is at `http://localhost:8000/docs`. If a host already uses port 8000, change only the host side of `ports` (for example `18000:8000`) and use that port in commands.

Inspect structured events with:

```bash
docker compose logs -f api consumer
```

Stop and remove local data with:

```bash
docker compose down -v --remove-orphans
```

## Tests

The test profile uses the real PostgreSQL and Redis containers and disables Kafka publishing to keep test outcomes deterministic:

```bash
docker compose up -d postgres redis
docker compose run --rm migrate
docker compose --profile test run --rm tests
```

The suite covers creation, same-owner duplicate holds, a real concurrent race, expiry, confirm-after-expiry, confirmation, wrong owner, release, missing seats, mandatory idempotency keys, replay, key/body collision, list, health, and metrics.

## API examples

Create a hold (201 for a new hold, 200 for the same user's existing hold):

```bash
curl -i -X POST http://localhost:8000/seats/demo-seat-1/hold \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: hold-demo-001' \
  -d '{"user_id":"user-123"}'
```

Replay the identical command using the same key; the status and body are preserved and `Idempotent-Replay: true` is returned. Reusing that key for another body or endpoint returns 409.

```bash
curl -i -X POST http://localhost:8000/seats/demo-seat-1/confirm \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: confirm-demo-001' \
  -d '{"user_id":"user-123"}'

curl -fsS http://localhost:8000/seats/demo-seat-1
curl -fsS 'http://localhost:8000/seats?trip_id=demo-trip-1'
```

Release uses the same body at `POST /seats/{seat_id}/release`. All write endpoints require `Idempotency-Key`. Expected errors are 400 (missing key), 403 (wrong owner), 404 (seat missing), 409 (held/booked/expired/collision), and 422 (invalid body).

## Data model and correctness

`seats` stores the trip, operator, status, hold owner/timestamps, booked owner, and version. The state transition and both `audit_events` and `outbox_events` inserts commit atomically. `idempotency_records` binds a key to the SHA-256 fingerprint, exact status, and response body for ten minutes. An expired key may be reused. A Redis hold key uses the same eight-minute TTL for fast negative hints, but PostgreSQL decides every write.

Pessimistic row locking was selected because contention is localized to a single seat and a losing request should fail deterministically. Two simultaneous holds queue on the same row; the winner commits `held`, then the loser observes the committed owner and receives 409. This costs one database lock wait under contention but avoids distributed-lock lease expiry and split-brain risk. The database transaction is deliberately short and contains no network call.

The expiry worker locks overdue rows in batches using `SKIP LOCKED`, making multiple replicas safe. Command-time expiry handles the interval between the deadline and worker scan. The outbox publisher uses Kafka idempotent production; downstream consumers must still deduplicate because a crash after publish but before marking the row can redeliver.

## Design decisions and trade-offs

The hybrid saga keeps the business-critical workflow explicit without coupling Booking to notification delivery. PostgreSQL rather than Redis enforces the invariant because Redis failover, eviction, or an expired lease must not permit double booking. The cost is more state-machine and outbox/inbox machinery, eventual consistency, and business compensations such as refunds that can fail independently. [ADR 001](task1/ADR-001-saga-pattern.md) documents the rejected approaches and recovery matrix.

## Limitations

- Compose runs one API worker so its embedded workers are easy to demonstrate. In Kubernetes, expiry and outbox publishers should be separately deployed processes with leader-independent `SKIP LOCKED` batching.
- Redis is a TTL mirror; the ten-minute idempotency record is PostgreSQL-authoritative to guarantee atomic replay after process/Redis failure. A production read-through Redis cache can reduce replay-table reads.
- The sample consumer logs events and demonstrates transport, but a production Audit consumer would persist with an inbox record in one transaction.
- Authentication/authorization, rate limiting, TLS, secrets management, webhook signing, schema registry compatibility checks, tracing, dashboards, and backup/restore automation are deployment concerns outside this runnable slice.
- Demo seeding belongs only to local execution and is disabled in the test profile; production imports trip/seat inventory.


## Time spent

Enter actual human review/work time before submission; do not count tool runtime as candidate effort.

| Task | Time |
|---|---:|
| Task 1 architecture and ADR | `30 minutes` |
| Task 2 implementation and testing | `3 hours` |
| Task 3 incident response | `30 minutes` |
| Final review and packaging | `1 hours` |

## Future improvements

Add contract tests and a schema registry; move workers to independent deployments; cache completed idempotency responses in Redis; add OpenTelemetry traces and SLO dashboards; property-test the state machine; load-test hot-seat contention; test Kafka and database fault recovery with Toxiproxy; implement reconciliation/refund workflows; and deploy with network policies, PodDisruptionBudgets, autoscaling on queue age, and per-operator notification bulkheads.
