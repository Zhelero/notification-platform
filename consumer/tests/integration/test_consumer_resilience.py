import uuid

from consumer.tests.integration.helpers import (
    send_message,
    send_raw_bytes,
    wait_for_notification,
    assert_notification_count_after_delay,
)


async def test_consumer_skips_message_without_correlation_id(db_conn):
    await send_message({
        "user_id": 1,
        "channel": "email",
        "text": "hello"
    })

    await assert_notification_count_after_delay(db_conn, 0)


async def test_consumer_continues_after_invalid_payload(db_conn):
    await send_message({
        "user_id": 1,
        "channel": "email",
        "text": "invalid"
    })

    correlation_id = uuid.uuid4()

    await send_message({
        "correlation_id": str(correlation_id),
        "user_id": 1,
        "channel": "email",
        "text": "valid",
    })

    row = await wait_for_notification(db_conn, correlation_id)

    assert row["text"] == "valid"


async def test_consumer_survives_non_json_message(db_conn):
    # Raw bytes, not JSON at all. decode_event() is called explicitly
    # inside the per-message try/except in worker.py (rather than as an
    # AIOKafkaConsumer value_deserializer) precisely so a payload like
    # this gets logged and skipped instead of crashing the whole
    # `async for msg in consumer` loop.
    await send_raw_bytes(b"this is not json")

    correlation_id = uuid.uuid4()

    await send_message({
        "correlation_id": str(correlation_id),
        "user_id": 1,
        "channel": "email",
        "text": "still alive",
    })

    row = await wait_for_notification(db_conn, correlation_id)

    assert row["text"] == "still alive"


async def test_consumer_continues_after_duplicate_message(db_conn):
    correlation_id = uuid.uuid4()

    duplicate_payload = {
        "correlation_id": str(correlation_id),
        "user_id": 1,
        "channel": "email",
        "text": "original",
    }

    # First delivery is saved, second is a genuine duplicate that
    # save_notification() turns into a DuplicateNotificationError.
    await send_message(duplicate_payload)
    await send_message(duplicate_payload)

    row = await wait_for_notification(db_conn, correlation_id)
    assert row["text"] == "original"

    # The consumer should still be alive and processing after handling
    # the duplicate - not stuck, not crashed.
    next_correlation_id = uuid.uuid4()

    await send_message({
        "correlation_id": str(next_correlation_id),
        "user_id": 2,
        "channel": "email",
        "text": "after duplicate",
    })

    next_row = await wait_for_notification(db_conn, next_correlation_id)
    assert next_row["text"] == "after duplicate"

    count = await db_conn.fetchval(
        "SELECT COUNT(*) FROM notifications WHERE correlation_id = $1",
        correlation_id,
    )

    assert count == 1


async def test_consumer_continues_after_invalid_uuid(db_conn):
    # Invalid UUID should fail schema validation but not stop the consumer.
    await send_message({
        "correlation_id": "not-a-uuid",
        "user_id": 1,
        "channel": "email",
        "text": "invalid",
    })

    correlation_id = uuid.uuid4()

    await send_message({
        "correlation_id": str(correlation_id),
        "user_id": 1,
        "channel": "email",
        "text": "valid",
    })

    row = await wait_for_notification(db_conn, correlation_id)

    assert row["text"] == "valid"