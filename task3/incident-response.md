# Incident response for booking confirmation collapse

## Incident declaration and evidence

At 02:14, declare SEV-1 and assign incident commander, operations lead, and scribe. Confirmation fell from 94% to 31% for 15 minutes. Payment P99 rose from 180 ms to 14.2 s while its CPU fell to 12%, and 498 of 500 database connections are occupied. Kafka `booking.events` lag is 2.1 million, Booking heap is 1.8/2 GB, three Booking pods were OOMKilled, and Redis is handling 42,000 hold TTL operations/s versus a normal 800–1,200. Notification errors and the DLQ are both zero.

## Ranked causes

1. **Payment database connection-pool exhaustion — primary (very high probability).** The direct saturation signal is 498/500 connections. Low Payment CPU with 14.2-second P99 indicates waiting on a constrained downstream resource rather than computation. Likely triggers are leaked/long-running transactions, a slow or blocked query, or an unbounded retry fan-out.
2. **Kafka consumer backlog and Booking memory pressure — cascading (high probability).** Payment completions cannot advance promptly, so `booking.events` lag reaches 2.1 million. Booking consumers likely fetch or retain too many records/retries, driving heap to 1.8/2 GB and three OOMKills. Restarts/rebalances further reduce throughput.
3. **Retry/hold storm — amplifier (medium probability).** Redis traffic is 35–52 times baseline. Client retries or saga redelivery may be repeatedly refreshing/checking holds while confirmations stall. This can add Kafka traffic and Booking allocations, but it does not explain the nearly full Payment DB pool as well as cause 1.

## Blast radius

Payment-backed checkout and booking confirmation are severely degraded; some users see pending/timeouts and uncertain payment outcomes. New payment attempts risk duplication unless their idempotency keys are stable. Events that depend on `booking.events` are delayed, and repeated Booking OOMs make status updates intermittent.

Seat browsing and creating holds remain functional because Seat Inventory and its database are isolated from Payment; Redis is hot but no failure is reported. Already confirmed seats remain authoritative. Notification Service is healthy at 0% errors, although notifications for confirmations still buried in Kafka cannot start. The empty DLQ shows this is throughput/backpressure, not poison-message rejection.

## First 20 minutes

Run commands from the production context. Record outputs and timestamps in the incident channel; never delete the topic or flush Redis.

### Minute 0 to 5 — stabilize and preserve evidence

```bash
kubectl -n gohub get deploy,pods,hpa -l 'app in (booking,payment)' -o wide
kubectl -n gohub get events --sort-by=.lastTimestamp | tail -50
kubectl -n gohub top pods -l 'app in (booking,payment)'
kubectl -n gohub logs deploy/payment --since=20m | grep -E 'timeout|pool|connection|deadlock' | tail -200
kubectl -n gohub logs deploy/booking --previous --since=20m | tail -200
```

Freeze unrelated deployments. Enable the existing Payment circuit breaker at the gateway/Booking configuration and shed *new* checkout/payment starts with `503 Retry-After`, while keeping browse, hold, and status endpoints open. Do not blindly retry unknown payment attempts; require the original payment attempt ID.

```bash
kubectl -n gohub annotate deploy booking incident.gohub.io/change-freeze="INC-YYYYMMDD-0214" --overwrite
kubectl -n gohub set env deploy/booking PAYMENT_CIRCUIT_FORCE_OPEN=true PAYMENT_RETRY_ENABLED=false
kubectl -n gohub rollout status deploy/booking --timeout=120s
```

### Minute 5 to 10 — find and release the database bottleneck safely

Connect through the approved read-only/admin path and inspect before terminating anything:

```sql
SELECT state, wait_event_type, wait_event, count(*)
FROM pg_stat_activity WHERE datname = 'payment' GROUP BY 1,2,3 ORDER BY 4 DESC;

SELECT pid, usename, application_name, client_addr, state,
       now()-xact_start AS xact_age, now()-query_start AS query_age,
       wait_event_type, wait_event, left(query, 160) AS query
FROM pg_stat_activity
WHERE datname = 'payment' AND pid <> pg_backend_pid()
ORDER BY xact_start NULLS LAST LIMIT 50;

SELECT blocked.pid AS blocked_pid, blocker.pid AS blocker_pid,
       now()-blocker.xact_start AS blocker_xact_age, left(blocker.query,160) AS blocker_query
FROM pg_stat_activity blocked
JOIN pg_locks bl ON bl.pid=blocked.pid AND NOT bl.granted
JOIN pg_locks kl ON kl.locktype=bl.locktype AND kl.database IS NOT DISTINCT FROM bl.database
 AND kl.relation IS NOT DISTINCT FROM bl.relation AND kl.page IS NOT DISTINCT FROM bl.page
 AND kl.tuple IS NOT DISTINCT FROM bl.tuple AND kl.transactionid IS NOT DISTINCT FROM bl.transactionid
 AND kl.classid IS NOT DISTINCT FROM bl.classid AND kl.objid IS NOT DISTINCT FROM bl.objid
 AND kl.objsubid IS NOT DISTINCT FROM bl.objsubid AND kl.granted
JOIN pg_stat_activity blocker ON blocker.pid=kl.pid;
```

If the evidence shows leaked idle transactions older than five minutes from Payment, terminate only that validated set; this rolls back their transactions and is safer than restarting the database:

```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname='payment' AND application_name='payment-service'
  AND state='idle in transaction' AND now()-xact_start > interval '5 minutes';
```

If one blocking query is responsible, capture its query/plan and terminate that PID only. Do **not** raise `max_connections` first; that can exhaust memory and move the failure to PostgreSQL.

### Minute 10 to 15 — bound consumers and recover capacity

Inspect Kafka lag by partition and consumer membership:

```bash
kubectl -n kafka exec deploy/kafka-tools -- kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group booking-service
kubectl -n gohub get deploy booking -o yaml | grep -E 'resources:|memory:|MAX_POLL|FETCH|CONCURRENCY' -A8
kubectl -n gohub scale deploy/payment --replicas=6
kubectl -n gohub rollout status deploy/payment --timeout=120s
```

Scale Payment only after blocked/leaked connections are cleared and verify the per-pod pool budget keeps total potential connections below a safe database limit (for example, no more than 350 of 500 across all pods). Patch Booking consumer fetch/batch/concurrency to the pre-approved bounded incident values; temporarily raising memory is acceptable only as a restart-stability measure, not a fix.

```bash
kubectl -n gohub set env deploy/booking KAFKA_MAX_POLL_RECORDS=100 BOOKING_CONSUMER_CONCURRENCY=4
kubectl -n gohub set resources deploy/booking --limits=memory=3Gi --requests=memory=1Gi
kubectl -n gohub rollout status deploy/booking --timeout=120s
```

### Minute 15 to 20 — controlled recovery and validation

Re-enable payment traffic gradually (5%, then 25%, then 100% only if stable) through the existing feature flag/canary mechanism. Keep automatic payment retries disabled until reconciliation proves outcomes. Track pool use, Payment P99, oldest Kafka message age, confirmation rate, Booking RSS/restarts, and Redis operation rate every minute.

Success gates are: database connections declining below the operational threshold, no new OOMKills, Payment P99 trending toward baseline, and lag/oldest-message age decreasing for five consecutive minutes. If they fail, reopen the circuit and keep checkout shed. Reconcile every pending attempt by provider idempotency ID before retrying or refunding.

## Root cause hypothesis and confirmation

The most likely root cause is Payment Service retaining database connections during slow/blocked work or a retry fan-out, exhausting its 500-connection pool. Requests then wait 14.2 seconds despite 12% CPU. Pending results accumulate into 2.1 million Kafka messages; Booking retains excessive batches/retry state, reaches 1.8/2 GB, and OOM/rebalance cycles deepen the lag. Users retry, amplifying Redis hold operations to 42,000/s.

Confirm by correlating the first pool-rise timestamp with deploy/config/audit history; grouping `pg_stat_activity` by state, wait event, query fingerprint, and application/pod; checking pool acquisition-timeout and transaction-duration histograms; inspecting locks and slow query plans; and plotting Kafka oldest-message age, Booking allocation profiles, and Redis commands by caller. Refute the hypothesis if most connections are healthy/active with normal query time or if pool saturation follows rather than precedes Kafka lag.

## Post-incident architecture change

Add a Payment bulkhead and circuit breaker: enforce a database connection budget per pod whose fleet total stays below the database ceiling, short acquisition/statement/idle-transaction timeouts, and a bounded worker queue. When the bulkhead fills, persist the idempotent payment command and return pending instead of holding a connection or multiplying retries; the circuit opens on pool-acquisition latency and reconciles uncertain provider results asynchronously. This prevents one slow dependency from consuming all Payment and Booking resources while preserving browse/hold availability.
