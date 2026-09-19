from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .models import SeatStatus


class HoldRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=80)


class UserActionRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=80)


class SeatResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    seat_id: str
    trip_id: str
    operator_id: str
    status: SeatStatus
    hold_user_id: str | None = None
    hold_created_at: datetime | None = None
    expires_at: datetime | None = None
    booked_user_id: str | None = None


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    database: Literal["ok", "error"]
    redis: Literal["ok", "error"]


class ErrorResponse(BaseModel):
    detail: str
    code: str
    request_id: str
