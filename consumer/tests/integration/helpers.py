import asyncio
import uuid
import requests
import json
from aiokafka import AIOKafkaProducer

KAFKA_TOPIC = "notifications"
KAFKA_BOOTSTRAP = "localhost:19092"

def send_notification(user_id=123, channel="#alerts", text=None, correlation_id=None):
    text = text or f"hello-{uuid.uuid4()}"
    correlation_id = correlation_id or uuid.uuid4()

    response = requests.post(
        "http://127.0.0.1:8001/notify",
        json={
            "user_id": user_id,
            "channel": channel,
            "text": text,
            "correlation_id": str(correlation_id),
        },
    )

    assert response.status_code == 200

    data = response.json()

    return {
        "user_id": user_id,
        "channel": channel,
        "text": text,
        "correlation_id": uuid.UUID(data["correlation_id"]),
    }

async def send_message(payload: dict):
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    await producer.start()
    try:
        await producer.send_and_wait(KAFKA_TOPIC, payload)
    finally:
        await producer.stop()


async def send_raw_bytes(payload: bytes):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)

    await producer.start()
    try:
        await producer.send_and_wait(KAFKA_TOPIC, payload)
    finally:
        await producer.stop()


async def wait_for_notification(db_conn, correlation_id, timeout=5):
    for _ in range(timeout * 10):
        row = await db_conn.fetchrow(
            """
            SELECT correlation_id, user_id, channel, text
            FROM notifications
            WHERE correlation_id = $1
            """,
            correlation_id,
        )

        if row:
            return row

        await asyncio.sleep(0.1)

    raise AssertionError(
        f"Notification {correlation_id} was not processed"
    )

async def wait_for_notification_count(db_conn, expected_count, timeout=5):
    for _ in range(timeout * 10):
        count = await db_conn.fetchval(
            "SELECT COUNT(*) FROM notifications"
        )
        if count == expected_count:
            return
        await asyncio.sleep(0.1)

    raise AssertionError(
        f"Expected {expected_count} notifications"
    )

async def assert_notification_count_after_delay(db_conn, expected_count, delay=2):
    await asyncio.sleep(delay)

    count = await db_conn.fetchval("SELECT COUNT(*) FROM notifications")

    assert count == expected_count, (
        f"Expected {expected_count} notifications after {delay}s, got {count}"
    )