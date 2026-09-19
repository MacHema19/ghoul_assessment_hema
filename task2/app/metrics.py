from prometheus_client import Counter

HOLD_CREATED = Counter("seat_hold_created_total", "New seat holds created")
HOLD_EXPIRED = Counter("seat_hold_expired_total", "Seat holds expired")
HOLD_CONFIRMED = Counter("seat_hold_confirmed_total", "Seat holds confirmed")
HOLD_RELEASED = Counter("seat_hold_released_total", "Seat holds explicitly released")
WRITE_CONFLICT = Counter("seat_write_conflict_total", "Write conflicts", ["reason"])
IDEMPOTENCY_REPLAY = Counter("seat_idempotency_replay_total", "Idempotent response replays")
