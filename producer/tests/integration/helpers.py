import asyncio
import uuid
import pytest
import requests


def send_notification(user_id=123, channel="#alerts", text=None):
    text = text or f"hello-{uuid.uuid4()}"

    response = requests.post(
        "http://127.0.0.1:8001/notify",
        json={
            "user_id": user_id,
            "channel": channel,
            "text": text,
        },
    )

    assert response.status_code == 200

    data = response.json()

    return {
        "text": text,
        "correlation_id": uuid.UUID(data["correlation_id"]),
    }

async def wait_for_notification(conn, correlation_id, timeout=15):
    deadline = asyncio.get_event_loop().time() + timeout

    while asyncio.get_event_loop().time() < deadline:
        row = await conn.fetchrow(
            """
            SELECT user_id, channel, text, correlation_id
            FROM notifications
            WHERE correlation_id = $1
            """,
            correlation_id,
        )

        if row:
            return row

        await asyncio.sleep(0.5)

    pytest.fail(f"Notification {correlation_id} was not processed.")