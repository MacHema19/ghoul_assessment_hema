import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from .config import Settings
from .metrics import HOLD_CONFIRMED, HOLD_CREATED, HOLD_RELEASED, IDEMPOTENCY_REPLAY, WRITE_CONFLICT
from .models import AuditEvent, IdempotencyRecord, OutboxEvent, Seat, SeatStatus

log = logging.getLogger(__name__)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def request_hash(action: str, seat_id: str, user_id: str) -> str:
    raw = json.dumps({"action": action, "seat_id": seat_id, "user_id": user_id}, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def public_seat(seat: Seat) -> dict:
    return {
        "seat_id": seat.id,
        "trip_id": seat.trip_id,
        "operator_id": seat.operator_id,
        "status": seat.status.value,
        "hold_user_id": seat.hold_user_id,
        "hold_created_at": seat.hold_created_at.isoformat() if seat.hold_created_at else None,
        "expires_at": seat.hold_expires_at.isoformat() if seat.hold_expires_at else None,
        "booked_user_id": seat.booked_user_id,
    }


def _conflict(detail: str, code: str) -> HTTPException:
    WRITE_CONFLICT.labels(reason=code).inc()
    return HTTPException(status_code=409, detail={"detail": detail, "code": code})


def _event(db: Session, seat: Seat, action: str, actor: str) -> None:
    event_type = f"seat.{action}"
    payload = {
        "event_version": 1,
        "seat_id": seat.id,
        "trip_id": seat.trip_id,
        "operator_id": seat.operator_id,
        "user_id": actor,
        "action": action,
        "occurred_at": utcnow().isoformat(),
    }
    db.add(AuditEvent(seat_id=seat.id, action=action, actor=actor, details=payload))
    db.add(OutboxEvent(aggregate_id=seat.id, event_type=event_type, payload=payload))


def mutate(db: Session, settings: Settings, *, action: str, seat_id: str, user_id: str, key: str, request_id: str) -> tuple[int, dict, bool]:
    now = utcnow()
    fingerprint = request_hash(action, seat_id, user_id)
    # Serialized by PostgreSQL. The idempotency row and seat mutation commit together.
    existing = db.execute(select(IdempotencyRecord).where(IdempotencyRecord.key == key).with_for_update()).scalar_one_or_none()
    if existing and existing.expires_at > now:
        if existing.request_hash != fingerprint:
            raise _conflict("Idempotency-Key was already used with a different request", "idempotency_key_collision")
        IDEMPOTENCY_REPLAY.inc()
        return existing.status_code, existing.response_json, True
    if existing:
        db.delete(existing)
        db.flush()

    seat = db.execute(select(Seat).where(Seat.id == seat_id).with_for_update()).scalar_one_or_none()
    if not seat:
        raise HTTPException(status_code=404, detail={"detail": "Seat not found", "code": "seat_not_found"})

    # Expiry is enforced on every locked read as well as by the background reaper.
    if seat.status == SeatStatus.held and seat.hold_expires_at and seat.hold_expires_at <= now:
        expired_actor = seat.hold_user_id or "system"
        seat.status = SeatStatus.available
        seat.hold_user_id = seat.hold_created_at = seat.hold_expires_at = None
        seat.version += 1
        _event(db, seat, "expired", expired_actor)

    status_code = 200
    if action == "held":
        if seat.status == SeatStatus.booked:
            raise _conflict("Seat is already booked", "seat_already_booked")
        if seat.status == SeatStatus.held:
            if seat.hold_user_id != user_id:
                raise _conflict("Seat is held by another user", "seat_held_by_another_user")
            # Same owner gets the existing hold and the original expiry; no extension.
        else:
            seat.status = SeatStatus.held
            seat.hold_user_id = user_id
            seat.hold_created_at = now
            seat.hold_expires_at = now + timedelta(seconds=settings.hold_ttl_seconds)
            seat.version += 1
            _event(db, seat, "held", user_id)
            HOLD_CREATED.inc()
            status_code = 201
    elif action == "confirmed":
        if seat.status == SeatStatus.booked:
            if seat.booked_user_id != user_id:
                raise _conflict("Seat is already booked", "seat_already_booked")
        elif seat.status != SeatStatus.held:
            raise _conflict("An active hold is required", "hold_missing_or_expired")
        elif seat.hold_user_id != user_id:
            raise HTTPException(status_code=403, detail={"detail": "Hold belongs to another user", "code": "wrong_hold_owner"})
        else:
            seat.status = SeatStatus.booked
            seat.booked_user_id = user_id
            seat.hold_user_id = seat.hold_created_at = seat.hold_expires_at = None
            seat.version += 1
            _event(db, seat, "confirmed", user_id)
            HOLD_CONFIRMED.inc()
    elif action == "released":
        if seat.status == SeatStatus.booked:
            raise _conflict("A booked seat cannot be released by the hold endpoint", "seat_already_booked")
        if seat.status == SeatStatus.held and seat.hold_user_id != user_id:
            raise HTTPException(status_code=403, detail={"detail": "Hold belongs to another user", "code": "wrong_hold_owner"})
        if seat.status == SeatStatus.held:
            seat.status = SeatStatus.available
            seat.hold_user_id = seat.hold_created_at = seat.hold_expires_at = None
            seat.version += 1
            _event(db, seat, "released", user_id)
            HOLD_RELEASED.inc()
    else:
        raise ValueError(action)

    db.flush()
    response = public_seat(seat)
    db.add(IdempotencyRecord(
        key=key,
        request_hash=fingerprint,
        status_code=status_code,
        response_json=response,
        expires_at=now + timedelta(seconds=settings.idempotency_ttl_seconds),
    ))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        # Concurrent reuse of a key: retry through the now-committed record.
        return mutate(db, settings, action=action, seat_id=seat_id, user_id=user_id, key=key, request_id=request_id)
    log.info("seat state changed", extra={"request_id": request_id, "seat_id": seat_id, "user_id": user_id, "action": action})
    return status_code, response, False


def purge_old_idempotency(db: Session) -> None:
    db.execute(delete(IdempotencyRecord).where(IdempotencyRecord.expires_at <= utcnow()))
    db.commit()
