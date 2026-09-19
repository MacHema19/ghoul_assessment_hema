import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, Query, Request, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis
from redis import Redis as SyncRedis
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from .config import get_settings
from .database import SessionLocal, get_db
from .logging import configure_logging
from .models import Seat, SeatStatus
from .schemas import HealthResponse, HoldRequest, SeatResponse, UserActionRequest
from .service import mutate, public_seat, request_hash
from .workers import expiry_worker, outbox_worker

settings = get_settings()
configure_logging(settings.log_level)
log = logging.getLogger(__name__)
redis = Redis.from_url(settings.redis_url, decode_responses=True, socket_connect_timeout=0.2, socket_timeout=0.2)
redis_sync = SyncRedis.from_url(settings.redis_url, decode_responses=True, socket_connect_timeout=0.2, socket_timeout=0.2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.seed_demo_data:
        with SessionLocal() as db:
            if not db.get(Seat, "demo-seat-1"):
                db.add_all([
                    Seat(id="demo-seat-1", trip_id="demo-trip-1", operator_id="demo-operator", status=SeatStatus.available),
                    Seat(id="demo-seat-2", trip_id="demo-trip-1", operator_id="demo-operator", status=SeatStatus.available),
                ])
                db.commit()
    stop = asyncio.Event()
    tasks = [asyncio.create_task(expiry_worker(settings, stop, redis)), asyncio.create_task(outbox_worker(settings, stop))]
    yield
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    await redis.aclose()


app = FastAPI(title="GoHub Seat Inventory Service", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def request_context(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    request.state.request_id = request_id
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    value = exc.detail if isinstance(exc.detail, dict) else {"detail": str(exc.detail), "code": "http_error"}
    return JSONResponse(status_code=exc.status_code, content={**value, "request_id": request.state.request_id})


def required_key(idempotency_key: str | None = Header(None, alias="Idempotency-Key")) -> str:
    if not idempotency_key or len(idempotency_key) > 200:
        raise HTTPException(status_code=400, detail={"detail": "A valid Idempotency-Key header is required", "code": "idempotency_key_required"})
    return idempotency_key


def write_action(action: str, seat_id: str, user_id: str, key: str, request_id: str, db: Session) -> JSONResponse:
    fingerprint = request_hash(action, seat_id, user_id)
    cache_key = f"idempotency:{key}"
    try:
        cached_raw = redis_sync.get(cache_key)
        if cached_raw:
            cached = json.loads(cached_raw)
            if cached["request_hash"] != fingerprint:
                raise HTTPException(status_code=409, detail={"detail": "Idempotency-Key was already used with a different request", "code": "idempotency_key_collision"})
            return JSONResponse(status_code=cached["status_code"], content=cached["response_json"], headers={"Idempotent-Replay": "true"})
    except HTTPException:
        raise
    except Exception:
        log.warning("Redis replay cache unavailable", extra={"request_id": request_id, "seat_id": seat_id, "user_id": user_id, "action": action})
    status_code, payload, replay = mutate(db, settings, action=action, seat_id=seat_id, user_id=user_id, key=key, request_id=request_id)
    try:
        redis_sync.set(cache_key, json.dumps({"request_hash": fingerprint, "status_code": status_code, "response_json": payload}), ex=settings.idempotency_ttl_seconds)
    except Exception:
        log.warning("Redis replay cache write failed", extra={"request_id": request_id, "seat_id": seat_id, "user_id": user_id, "action": action})
    headers = {"Idempotent-Replay": "true"} if replay else {}
    return JSONResponse(status_code=status_code, content=payload, headers=headers)


@app.post("/seats/{seat_id}/hold", response_model=SeatResponse, responses={409: {}})
def hold(seat_id: str, body: HoldRequest, request: Request, key: str = Depends(required_key), db: Session = Depends(get_db)):
    response = write_action("held", seat_id, body.user_id, key, request.state.request_id, db)
    # Redis mirrors the TTL for fast availability hints; PostgreSQL remains authoritative.
    if response.status_code == 201:
        try:
            redis_sync.set(f"seat-hold:{seat_id}", body.user_id, ex=settings.hold_ttl_seconds)
        except Exception:
            log.warning("Redis TTL mirror unavailable", extra={"seat_id": seat_id, "user_id": body.user_id, "action": "held"})
    return response


@app.post("/seats/{seat_id}/confirm", response_model=SeatResponse)
def confirm(seat_id: str, body: UserActionRequest, request: Request, key: str = Depends(required_key), db: Session = Depends(get_db)):
    return write_action("confirmed", seat_id, body.user_id, key, request.state.request_id, db)


@app.post("/seats/{seat_id}/release", response_model=SeatResponse)
def release(seat_id: str, body: UserActionRequest, request: Request, key: str = Depends(required_key), db: Session = Depends(get_db)):
    return write_action("released", seat_id, body.user_id, key, request.state.request_id, db)


@app.get("/seats/{seat_id}", response_model=SeatResponse)
def get_seat(seat_id: str, db: Session = Depends(get_db)):
    seat = db.get(Seat, seat_id)
    if not seat:
        raise HTTPException(status_code=404, detail={"detail": "Seat not found", "code": "seat_not_found"})
    return public_seat(seat)


@app.get("/seats", response_model=list[SeatResponse])
def list_seats(trip_id: str = Query(min_length=1), db: Session = Depends(get_db)):
    return [public_seat(s) for s in db.scalars(select(Seat).where(Seat.trip_id == trip_id).order_by(Seat.id))]


@app.get("/health", response_model=HealthResponse)
async def health():
    db_ok = redis_ok = True
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
    except Exception:
        db_ok = False
    try:
        await redis.ping()
    except Exception:
        redis_ok = False
    payload = {"status": "ok" if db_ok and redis_ok else "degraded", "database": "ok" if db_ok else "error", "redis": "ok" if redis_ok else "error"}
    return JSONResponse(status_code=200 if db_ok and redis_ok else 503, content=payload)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
