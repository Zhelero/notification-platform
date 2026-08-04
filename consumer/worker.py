import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from consumer.logging_config import setup_logging
from consumer.config import KAFKA_BOOTSTRAP, TOPIC
from consumer.db import get_pool, save_notification


setup_logging()
logger = logging.getLogger(__name__)

async def main():
    pool = await get_pool()

    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='notification-workers',
        auto_offset_reset='earliest',
    )

    await consumer.start()

    logger.info("Consumer started")

    try:
        async for msg in consumer:
            try:
                event = msg.value

                logger.info(
                    "Message received",
                    extra={
                        "correlation_id": event.get("correlation_id"),
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                    },
                )

                await save_notification(pool, event)

                logger.info(
                    "Notification saved",
                    extra={
                        "correlation_id": event.get("correlation_id"),
                        "topic": msg.topic,
                        "user_id": event.get("user_id"),
                        "channel": event.get("channel"),
                        "partition": msg.partition,
                        "offset": msg.offset,
                    },
                )

            except Exception:
                logger.exception(
                    "Failed to process message",
                    extra={
                        "correlation_id": event.get("correlation_id"),
                        "topic": msg.topic,
                        "partition": msg.partition,
                        "offset": msg.offset,
                    },
                )

    finally:
        logger.info("Consumer stopped")
        await consumer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())