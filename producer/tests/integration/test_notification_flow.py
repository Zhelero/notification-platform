import uuid

from producer.tests.integration.helpers import (
    send_notification,
    wait_for_notification,
    wait_for_notifications,
)

async def test_notification_is_processed_end_to_end(db_conn):
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


async def test_multiple_messages(db_conn):
    events = [
        send_notification(
            user_id=i,
            channel=f"#channel-{i}",
            text=f"multi-{i}-{uuid.uuid4()}")
        for i in range(8)
    ]
    correlation_ids = [event["correlation_id"] for event in events]

    rows = await wait_for_notifications(db_conn, correlation_ids)

    assert len(rows) == len(events)

    for event in events:
        row = rows[event["correlation_id"]]
        assert row["user_id"] == event["user_id"]
        assert row["channel"] == event["channel"]
        assert row["text"] == event["text"]
        assert row["correlation_id"] == event["correlation_id"]


async def test_different_users_are_not_mixed_up(db_conn):
    events = [
        send_notification(user_id=1, channel="#alerts", text=f"user1-{uuid.uuid4()}"),
        send_notification(user_id=2, channel="#general", text=f"user2-{uuid.uuid4()}"),
        send_notification(user_id=3, channel="#random", text=f"user3-{uuid.uuid4()}"),
    ]
    correlation_ids = [event["correlation_id"] for event in events]

    rows = await wait_for_notifications(db_conn, correlation_ids)

    for event in events:
        row = rows[event["correlation_id"]]
        assert row["user_id"] == event["user_id"]
        assert row["channel"] == event["channel"]
        assert row["text"] == event["text"]
        assert row["correlation_id"] == event["correlation_id"]


async def test_correlation_id_is_unique_per_message(db_conn):
    events = [send_notification() for _ in range(10)]
    correlation_ids = [event["correlation_id"] for event in events]

    assert len(correlation_ids) == len(set(correlation_ids))

    rows = await wait_for_notifications(db_conn, correlation_ids)

    assert set(rows.keys()) == set(correlation_ids)


async def test_notifications_are_persisted_once(db_conn):
    events = [send_notification() for _ in range(5)]
    correlation_ids = [event["correlation_id"] for event in events]

    rows = await wait_for_notifications(db_conn, correlation_ids)

    assert set(rows.keys()) == set(correlation_ids)

    count = await db_conn.fetchval(
        "SELECT COUNT(*) FROM notifications"
    )

    assert count == len(events)