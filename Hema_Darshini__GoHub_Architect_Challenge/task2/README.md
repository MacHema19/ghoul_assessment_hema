# Seat Inventory Service

This service is the authority for seat state. PostgreSQL row locks serialize mutations; a worker expires holds from the persisted timestamp; Redis mirrors active holds for low-latency hints but is never trusted for booking correctness. See the root README for full run commands, API examples, and trade-offs.

Run from this directory with `docker compose up --build -d`, then open `http://localhost:8000/docs`. Apply schema changes with `docker compose run --rm migrate`; run integration tests with `docker compose --profile test run --rm tests` while PostgreSQL and Redis are running.
