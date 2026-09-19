# Submission checklist

## Assessment coverage

- [x] C4-style container diagram labels all five services, Kafka, per-service stores, Redis, and the eight-minute hold mechanism.
- [x] ADR uses the required Title, Status, Context, Decision, Consequences, and Alternatives structure; chooses a hybrid saga and documents compensation, recovery, and rejected alternatives.
- [x] Seat API implements all three writes, both reads, health, and Prometheus metrics with typed models and correct error classes.
- [x] Seat records include trip, operator, hold owner, creation, expiry, booked owner, and status.
- [x] Default hold is exactly eight minutes; a database worker expires holds automatically and command-time checks close the deadline race.
- [x] Every write requires `Idempotency-Key`; successful status/body replay is persisted for ten minutes and key/request collisions return 409.
- [x] PostgreSQL `SELECT FOR UPDATE` prevents double holds/bookings; a two-thread integration test proves one winner.
- [x] Structured state-change logs include request context through the response header and include seat, user, and action fields; worker/event logs include the same domain fields.
- [x] Transactional outbox publishes `seat.held`, `seat.confirmed`, `seat.released`, and `seat.expired`; a consumer logs them.
- [x] Audit rows are committed with every state transition.
- [x] Test suite has at least ten cases and covers every case requested in the prompt.
- [x] Incident response ranks pool exhaustion first and explicitly uses 498/500, 14.2 s, 2.1 M, 1.8/2 GB, OOMKills, and 42k ops/s.
- [x] Incident mitigation is time-ordered and includes executable Kubernetes and PostgreSQL checks plus safety gates.
- [x] Root README includes overview, architecture, assumptions, run/test/API instructions, decisions, TTL/concurrency/idempotency, limitations, AI disclosure, time placeholders, and improvements.

## Automatic-disqualification audit

- [x] Compose build/up, health, metrics, test, smoke, and concurrency commands are executed before packaging; results are recorded below.
- [x] Concurrency protection is implemented and tested, not merely described.
- [x] ADR includes costs, risks, compensations, and three rejected alternatives.
- [x] AI use is disclosed accurately; candidate review/ownership is explicitly required because the assessment restricts AI architecture decisions.
- [x] No third-party solution or unattributed non-trivial borrowed code is included.

## Validation record

This section is filled from the final executed validation rather than predicted results.

- Compose clean/build/up: `PASS` — final run used `down -v`, `build --no-cache`, and `up -d --wait`.
- Container health/status: `PASS` — API, PostgreSQL, Redis, and Kafka healthy; consumer running; migration exited 0.
- Health endpoint: `PASS` — HTTP 200 with database and Redis both `ok`.
- Metrics endpoint: `PASS` — HTTP 200 with Prometheus `seat_hold_created_total` and required lifecycle counters.
- Pytest: `PASS` — 16 passed in 2.46 seconds; one upstream Starlette/AnyIO deprecation warning.
- API smoke test: `PASS` — hold 201, same-key replay 201 with replay header, confirm 200/booked.
- Concurrent hold test: `PASS` — simultaneous independent users returned exactly `[201, 409]`.

## Before sending

- [ ] Candidate name is confirmed; the assessment mandates `Hema_Darshini_GoHub_Architect_Challenge.zip`, which is used unless the candidate supplies a correction.
- [ ] Candidate replaces time-spent placeholders with actual human time.
- [ ] Candidate independently reviews and can defend the architecture and incident decisions.
- [ ] ZIP opens cleanly and contains only the documented submission files.
