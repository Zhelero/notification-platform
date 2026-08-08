import asyncio
import uuid
import pytest
import requests


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

def post_notify(payload: dict):
    """POST a raw payload to /notify without asserting success.

    Use this instead of send_notification() for negative-path tests,
    e.g. sending a payload that's missing a required field and
    expecting a 422 rather than a 200.
    """
    return requests.post("http://127.0.0.1:8001/notify", json=payload)

async def wait_for_notification(conn, correlation_id, timeout=15):
    rows = await wait_for_notifications(
        conn,
        [correlation_id],
        timeout=timeout,
    )

    return rows[correlation_id]

async def wait_for_notifications(conn, correlation_ids, timeout=15):
    """
    Wait until all notifications with the given correlation IDs are stored.

    Returns a dict {correlation_id: row}. Fails if any notification
    is not processed before the timeout.
    """
    pending = set(correlation_ids)
    found = {}
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while pending and loop.time() < deadline:
        rows = await conn.fetch(
            """
            SELECT user_id, channel, text, correlation_id
            FROM notifications
            WHERE correlation_id = ANY($1::uuid[])
            """,
            list(pending),
        )

        for row in rows:
            found[row["correlation_id"]] = row
            pending.discard(row["correlation_id"])

        if pending:
            await asyncio.sleep(0.5)

    if pending:
        pytest.fail(f"Notifications were not processed in time: {pending}")

    return found

async def assert_notification_count_after_delay(db_conn, expected_count, delay=2):
    await asyncio.sleep(delay)

    count = await db_conn.fetchval("SELECT COUNT(*) FROM notifications")

    assert count == expected_count, (
        f"Expected {expected_count} notifications after {delay}s, got {count}"
    )