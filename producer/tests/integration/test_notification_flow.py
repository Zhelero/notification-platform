import uuid

from producer.tests.integration.helpers import send_notification, wait_for_notification

async def test_notification_flow(db_conn):
    event = send_notification(
        user_id=42,
        channel="#test",
        text=f"integration-{uuid.uuid4()}",
    )

    row = await wait_for_notification(db_conn, event["correlation_id"])

    assert row["user_id"] == 42
    assert row["channel"] == "#test"
    assert row["text"] == event["text"]
    assert row["correlation_id"] == event["correlation_id"]
