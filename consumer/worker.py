import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer
from datetime import datetime, UTC

from elastic import index_log
from logging_config import setup_logging
from config import KAFKA_BOOTSTRAP, TOPIC
from db import get_pool, save_notification


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
            event = msg.value

            logger.info(
                "Message received",
                extra={
                    "correlation_id": event["correlation_id"],
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                },
            )

            await save_notification(pool, event)

            logger.info(
                "Notification saved",
                extra={
                    "correlation_id": event["correlation_id"],
                    "topic": msg.topic,
                    "user_id": event["user_id"],
                    "channel": event["channel"],
                },
            )

    finally:
        await consumer.stop()
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())