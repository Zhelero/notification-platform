import logging
import json
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from aiokafka import AIOKafkaProducer

from producer.schemas import NotifyRequest
from producer.config import KAFKA_BOOTSTRAP, TOPIC
from producer.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Kafka producer...")

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )
    await producer.start()

    logger.info("Kafka producer started")

    app.state.producer = producer

    yield

    logger.info("Stopping Kafka producer")
    await producer.stop()
    logger.info("Kafka producer stopped")


app = FastAPI(lifespan=lifespan)


@app.post("/notify")
async def notify(request: Request, payload: NotifyRequest):
    correlation_id = str(uuid.uuid4())

    event = {
        "correlation_id": correlation_id,
        **payload.model_dump(),
    }

    logger.info(
        "Notification request received",
        extra={
            "correlation_id": correlation_id,
            "user_id": payload.user_id,
            "channel": payload.channel,
        },
    )

    producer = request.app.state.producer

    try:
        await producer.send_and_wait(TOPIC, event)

        logger.info(
            "Notification published",
            extra={
                "correlation_id": correlation_id,
                "user_id": payload.user_id,
                "channel": payload.channel,
                "topic": TOPIC,
            },
        )

    except Exception:
        logger.exception(
            "Failed to publish notification",
            extra={
                "correlation_id": correlation_id,
                "user_id": payload.user_id,
                "channel": payload.channel,
            },
        )
        raise HTTPException(
            status_code=503,
            detail="Notification broker unavailable",
        )

    return {
        "status": "published",
        "correlation_id": correlation_id,
    }