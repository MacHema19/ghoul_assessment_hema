import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from .config import get_settings
from .logging import configure_logging


async def consume() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    log = logging.getLogger("seat-event-consumer")
    consumer = AIOKafkaConsumer(settings.kafka_topic, bootstrap_servers=settings.kafka_bootstrap_servers, group_id="seat-audit-log", auto_offset_reset="earliest")
    await consumer.start()
    try:
        async for message in consumer:
            event = json.loads(message.value)
            log.info("event consumed", extra={"event_id": event["event_id"], "event_type": event["event_type"], "seat_id": event["seat_id"], "user_id": event["user_id"], "action": "consume"})
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(consume())
