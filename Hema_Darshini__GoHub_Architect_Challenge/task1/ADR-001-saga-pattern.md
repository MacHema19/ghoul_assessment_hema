# ADR 001 Hybrid saga with Booking owned orchestration

**Status:** Accepted

## Context

GoHub must reserve a seat, take payment asynchronously, confirm the booking, notify an operator within 30 seconds, and retain an audit trail while serving 50-plus operators and 100,000-plus daily active users. Seat uniqueness is a hard invariant, but payment and operator systems can be slow or unavailable. A Payment Service outage must not stop browsing or the creation of an eight-minute hold. There is no safe distributed ACID transaction across the Booking, Seat Inventory, Payment, and Notification databases.

The difficult boundary is between an expiring seat hold and an uncertain payment result. The system needs an explicit owner for the workflow, deterministic compensation, bounded retries, and enough event decoupling that downstream failures remain isolated.

## Decision

Use a **hybrid saga**. Booking Service is the durable orchestrator for the critical transaction through `HOLD_CREATED → PAYMENT_PENDING → PAYMENT_AUTHORIZED → CONFIRMING_SEAT → CONFIRMED`, with terminal `FAILED`, `EXPIRED`, and `MANUAL_REVIEW` states. It sends payment and seat commands asynchronously through Kafka and consumes their results. Notification and Audit are choreographed consumers of immutable domain events; they are not allowed to decide whether the booking succeeded.

Each service owns a PostgreSQL database. Every state change writes the aggregate and an outbox event in one local transaction. Outbox relays publish to Kafka with an event ID, aggregate ID, schema version, causation ID, correlation/booking ID, and occurred-at time. Consumers deduplicate event IDs in an inbox table. Topics are partitioned by booking ID (seat events may be keyed by seat ID), preserving per-aggregate order without claiming global order.

Seat Inventory is the sole authority for seat availability. `SELECT … FOR UPDATE` serializes changes to a seat row and the data model prevents confirmation without a live hold owned by the requesting user. It stores the eight-minute `expires_at` in PostgreSQL; a `SKIP LOCKED` worker transitions overdue holds to available and publishes `seat.expired`. Redis holds an expiring mirror for fast hints and replay caching, but Redis eviction or outage cannot sell a seat twice. Every locked command also evaluates `expires_at`, closing the worker-delay race.

Payment is initiated only after a hold exists. The Booking Service never blocks a user request on the payment provider: it returns a pending booking and advances the saga from events. Payment commands carry a stable attempt ID and Payment Service uses that as its provider idempotency key. A payment timeout is an unknown outcome, not an immediate failure; the service reconciles with the provider before retrying or compensating.

Compensation and recovery are explicit:

| Failure | Recovery or compensation |
|---|---|
| Payment rejected before hold expiry | Mark booking failed; command Seat Inventory to release the hold. |
| Payment times out or result is unknown | Open circuit, reconcile by provider attempt ID, and do not create a second charge. Hold may expire while payment status remains pending. |
| Hold expires before payment authorization | Reject seat confirmation. If a late authorization arrives, issue an idempotent refund/void and mark the booking failed. |
| Seat confirmation fails after authorization | Retry confirmation while the hold is valid; after expiry, refund/void and route irreconcilable cases to manual review. |
| Notification webhook fails | Booking remains confirmed. Retry with exponential backoff and jitter until the 30-second deadline, then alert and continue durable retries/DLQ. |
| Kafka or consumer is unavailable | Local transactions continue where safe; outboxes drain after recovery. Lag SLOs trigger autoscaling and admission controls. |
| Duplicate or reordered delivery | Inbox deduplication plus valid-state-transition checks make handlers idempotent; stale events are ignored and audited. |

Operator notification starts from `booking.confirmed`. Notification Service signs webhooks, applies per-operator timeouts and circuit breakers, and persists each attempt. A high-priority consumer group, lag alert, and retry schedule target first delivery within 30 seconds. Audit Service consumes all transitions into append-only storage; the source services also retain local audit/outbox facts so an Audit outage does not erase history.

## Consequences

The critical workflow has one inspectable state machine and a clear recovery owner. Payment latency and outages are isolated from browse/hold traffic. Choreographed notification and audit consumers can scale independently, and adding a new observer does not enlarge the orchestrator. Database ownership is preserved and no service reads another service's tables.

This choice adds orchestration code, saga-state migrations, outbox/inbox tables, reconciliation jobs, and operational tooling. Kafka delivery is at least once, so every handler must be idempotent. Cross-service state is eventually consistent: a customer can briefly see `payment_pending`, and support tooling must explain intermediate states. A late payment may require a refund, which is a compensating business action rather than a rollback and can itself fail. Maintaining event schemas and correlation IDs becomes a governance obligation.

The orchestrator is a logical coordination point. It must be stateless above its database, horizontally replicated, partition-tolerant, and protected by optimistic state transitions so it does not become a single runtime failure point. The eight-minute window also couples operational SLOs: prolonged Kafka lag can cause legitimate holds to expire, so queue age matters more than message count alone.

## Alternatives considered

### Pure choreography

Services could react only to each other's events. This offers low central coupling and makes new observers easy to add, but the critical booking path becomes an implicit state machine distributed across topic subscriptions. Late payment, expired hold, refund, and retry interactions are difficult to reason about and audit, and circular event chains become likely. It was rejected for the core transaction, while retained for Notification and Audit where failure must not redefine booking success.

### Pure orchestration

Booking could command every participant, including each notification and audit write. This gives a single visible flow but couples the orchestrator to non-critical observers and makes notification latency/failure part of the booking control plane. It also creates needless command traffic and limits independent evolution. It was rejected in favor of events for side effects that do not determine the transaction outcome.

### Synchronous request chain or distributed transaction

A REST chain from Booking to Payment to Seat to Notification is simple in a demo but violates payment decoupling, amplifies tail latency, and allows one outage to consume upstream threads and connections. XA/two-phase commit is poorly supported across Kafka, external payment providers, and separately owned stores; it also holds resources across network failures. Both were rejected because they reduce availability and still cannot atomically include an external payment provider.
