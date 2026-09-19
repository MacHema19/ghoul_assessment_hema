import asyncio
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from redis.asyncio import Redis
from sqlalchemy import select

from .config import Settings
from .database import SessionLocal
from .metrics import HOLD_EXPIRED
from .models import OutboxEvent, Seat, SeatStatus
from .service import _event, purge_old_idempotency

log = logging.getLogger(__name__)


async def expiry_worker(settings: Settings, stop: asyncio.Event, redis: Redis) -> None:
    while not stop.is_set():
        try:
            with SessionLocal() as db:
                now = datetime.now(timezone.utc)
                seats = db.execute(
                    select(Seat).where(Seat.status == SeatStatus.held, Seat.hold_expires_at <= now)
                    .with_for_update(skip_locked=True).limit(100)
                ).scalars().all()
                for seat in seats:
                    actor = seat.hold_user_id or "system"
                    seat.status = SeatStatus.available
                    seat.hold_user_id = seat.hold_created_at = seat.hold_expires_at = None
                    seat.version += 1
                    _event(db, seat, "expired", actor)
                    HOLD_EXPIRED.inc()
                    await redis.delete(f"seat-hold:{seat.id}")
                    log.info("hold expired", extra={"request_id": "expiry-worker", "seat_id": seat.id, "user_id": actor, "action": "expired"})
                purge_old_idempotency(db)
                db.commit()
        except Exception:
            log.exception("expiry worker iteration failed", extra={"action": "expiry_worker"})
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.expiry_poll_seconds)
        except TimeoutError:
            pass


async def outbox_worker(settings: Settings, stop: asyncio.Event) -> None:
    if not settings.enable_kafka:
        return
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers, enable_idempotence=True)
    while not stop.is_set():
        try:
            await producer.start()
            break
        except Exception:
            log.warning("Kafka unavailable; retrying", extra={"action": "outbox_connect"})
            await asyncio.sleep(2)
    try:
        while not stop.is_set():
            with SessionLocal() as db:
                rows = db.execute(
                    select(OutboxEvent).where(OutboxEvent.published_at.is_(None))
                    .order_by(OutboxEvent.created_at).with_for_update(skip_locked=True).limit(100)
                ).scalars().all()
                for event in rows:
                    envelope = {"event_id": str(event.id), "event_type": event.event_type, **event.payload}
                    await producer.send_and_wait(settings.kafka_topic, json.dumps(envelope).encode(), key=event.aggregate_id.encode())
                    event.published_at = datetime.now(timezone.utc)
                    event.attempts += 1
                    log.info("event published", extra={"event_id": event.id, "event_type": event.event_type, "action": "publish"})
                db.commit()
            await asyncio.sleep(0.25 if rows else 1)
    except Exception:
        log.exception("outbox worker stopped", extra={"action": "outbox_publish"})
    finally:
        await producer.stop()
