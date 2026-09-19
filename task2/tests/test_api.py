import time
from concurrent.futures import ThreadPoolExecutor

from conftest import headers


def test_hold_creation(client):
    response = client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h1"))
    assert response.status_code == 201
    assert response.json()["status"] == "held"
    assert response.json()["expires_at"]


def test_same_user_hold_is_idempotent_without_extending(client):
    first = client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h1"))
    second = client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h2"))
    assert second.status_code == 200
    assert second.json()["expires_at"] == first.json()["expires_at"]


def test_double_hold_prevented(client):
    assert client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h1")).status_code == 201
    response = client.post("/seats/seat-1/hold", json={"user_id": "u2"}, headers=headers("h2"))
    assert response.status_code == 409
    assert response.json()["code"] == "seat_held_by_another_user"


def test_real_concurrent_hold_has_single_winner(client):
    def hold(user: str):
        return client.post("/seats/seat-1/hold", json={"user_id": user}, headers=headers(f"concurrent-{user}"))
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(hold, ["u1", "u2"]))
    assert sorted(r.status_code for r in responses) == [201, 409]
    assert client.get("/seats/seat-1").json()["hold_user_id"] in {"u1", "u2"}


def test_expiry_automatically_releases(client):
    client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h1"))
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and client.get("/seats/seat-1").json()["status"] != "available":
        time.sleep(0.05)
    assert client.get("/seats/seat-1").json()["status"] == "available"


def test_confirm_expired_returns_409(client):
    client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h1"))
    time.sleep(1.1)
    response = client.post("/seats/seat-1/confirm", json={"user_id": "u1"}, headers=headers("c1"))
    assert response.status_code == 409
    assert response.json()["code"] == "hold_missing_or_expired"


def test_confirm_active_hold(client):
    client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h1"))
    response = client.post("/seats/seat-1/confirm", json={"user_id": "u1"}, headers=headers("c1"))
    assert response.status_code == 200
    assert response.json()["status"] == "booked"


def test_wrong_owner_cannot_confirm(client):
    client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h1"))
    response = client.post("/seats/seat-1/confirm", json={"user_id": "u2"}, headers=headers("c1"))
    assert response.status_code == 403


def test_release(client):
    client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("h1"))
    response = client.post("/seats/seat-1/release", json={"user_id": "u1"}, headers=headers("r1"))
    assert response.status_code == 200
    assert response.json()["status"] == "available"


def test_missing_seat(client):
    assert client.get("/seats/missing").status_code == 404
    assert client.post("/seats/missing/hold", json={"user_id": "u1"}, headers=headers("h1")).status_code == 404


def test_idempotency_replays_original(client):
    first = client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("same"))
    second = client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("same"))
    assert second.status_code == first.status_code == 201
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replay"] == "true"


def test_idempotency_collision(client):
    client.post("/seats/seat-1/hold", json={"user_id": "u1"}, headers=headers("same"))
    response = client.post("/seats/seat-2/hold", json={"user_id": "u1"}, headers=headers("same"))
    assert response.status_code == 409
    assert response.json()["code"] == "idempotency_key_collision"


def test_write_requires_idempotency_key(client):
    assert client.post("/seats/seat-1/hold", json={"user_id": "u1"}).status_code == 400


def test_list_by_trip(client):
    response = client.get("/seats", params={"trip_id": "trip-1"})
    assert response.status_code == 200
    assert [s["seat_id"] for s in response.json()] == ["seat-1", "seat-2"]


def test_health(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok", "redis": "ok"}


def test_metrics(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "seat_hold_created_total" in response.text
