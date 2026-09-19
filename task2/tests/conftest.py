import os

os.environ["ENABLE_KAFKA"] = "false"
os.environ["SEED_DEMO_DATA"] = "false"
os.environ["HOLD_TTL_SECONDS"] = "1"
os.environ["EXPIRY_POLL_SECONDS"] = "0.05"

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.database import SessionLocal
from app.main import app, redis_sync
from app.models import AuditEvent, IdempotencyRecord, OutboxEvent, Seat, SeatStatus


@pytest.fixture(autouse=True)
def clean_database():
    redis_sync.flushdb()
    with SessionLocal() as db:
        for model in (OutboxEvent, AuditEvent, IdempotencyRecord, Seat):
            db.execute(delete(model))
        db.add_all([
            Seat(id="seat-1", trip_id="trip-1", operator_id="op-1", status=SeatStatus.available),
            Seat(id="seat-2", trip_id="trip-1", operator_id="op-1", status=SeatStatus.available),
        ])
        db.commit()
    yield


@pytest.fixture
def client():
    with TestClient(app) as value:
        yield value


def headers(key: str) -> dict[str, str]:
    return {"Idempotency-Key": key}
