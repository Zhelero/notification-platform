import uuid

from producer.tests.integration.helpers import (
    send_notification,
    wait_for_notification,
    wait_for_notifications,
    post_notify,
    assert_notification_count_after_delay,
)

class TestEndToEndFlow:
    async def test_notification_is_processed_end_to_end(self, db_conn):
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

    async def test_multiple_messages(self, db_conn):
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

    async def test_large_batch_processing(self, db_conn):
        events = [
            send_notification(
                user_id=i,
                channel=f"#batch-{i}",
                text=f"batch-{i}-{uuid.uuid4()}",
            )
            for i in range(50)
        ]

        correlation_ids = [event["correlation_id"] for event in events]

        rows = await wait_for_notifications(db_conn, correlation_ids)

        assert len(rows) == len(events)

        for event in events:
            row = rows[event["correlation_id"]]
            assert row["text"] == event["text"]

        count = await db_conn.fetchval(
            "SELECT COUNT(*) FROM notifications"
        )

        assert count == len(events)

    async def test_notifications_are_persisted_once(self, db_conn):
        events = [send_notification() for _ in range(5)]
        correlation_ids = [event["correlation_id"] for event in events]

        rows = await wait_for_notifications(db_conn, correlation_ids)

        assert set(rows.keys()) == set(correlation_ids)

        count = await db_conn.fetchval(
            "SELECT COUNT(*) FROM notifications"
        )

        assert count == len(events)


class TestDataIntegrity:
    async def test_different_users_are_not_mixed_up(self, db_conn):
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

    async def test_unicode_text_is_preserved(self, db_conn):
        event = send_notification(
            user_id=42,
            channel="#test",
            text="Привет 👋 สวัสดี <test>",
        )

        row = await wait_for_notification(db_conn, event["correlation_id"])

        assert row["user_id"] == event["user_id"]
        assert row["channel"] == event["channel"]
        assert row["text"] == event["text"]
        assert row["correlation_id"] == event["correlation_id"]

    async def test_long_unicode_text_is_preserved(self, db_conn):
        text = ("Привет 👋 สวัสดี こんにちは 🚀 " * 100).strip()

        event = send_notification(
            user_id=42,
            channel="#test",
            text=text,
        )

        row = await wait_for_notification(db_conn, event["correlation_id"])

        assert row["user_id"] == event["user_id"]
        assert row["channel"] == event["channel"]
        assert row["text"] == event["text"]
        assert row["correlation_id"] == event["correlation_id"]


class TestMessageIdentity:
    async def test_correlation_id_is_unique_per_message(self, db_conn):
        events = [send_notification() for _ in range(10)]
        correlation_ids = [event["correlation_id"] for event in events]

        assert len(correlation_ids) == len(set(correlation_ids))

        rows = await wait_for_notifications(db_conn, correlation_ids)

        assert set(rows.keys()) == set(correlation_ids)

    async def test_same_payload_creates_distinct_notifications(self, db_conn):
        event1 = send_notification(
            user_id=1,
            channel="#test",
            text="same-message",
        )

        event2 = send_notification(
            user_id=1,
            channel="#test",
            text="same-message",
        )

        rows = await wait_for_notifications(
            db_conn,
            [event1["correlation_id"], event2["correlation_id"]],
        )

        assert event1["correlation_id"] != event2["correlation_id"]

        assert rows[event1["correlation_id"]]["text"] == "same-message"
        assert rows[event2["correlation_id"]]["text"] == "same-message"

        count = await db_conn.fetchval(
            "SELECT COUNT(*) FROM notifications"
        )

        assert count == 2

    async def test_duplicate_message_is_processed_once(self, db_conn):
        correlation_id = uuid.uuid4()

        event1 = send_notification(
            user_id=42,
            channel="#test",
            text="duplicate message",
            correlation_id=correlation_id,
        )

        event2 = send_notification(
            user_id=42,
            channel="#test",
            text="duplicate message",
            correlation_id=correlation_id,
        )

        row = await wait_for_notification(db_conn, correlation_id)

        assert row["user_id"] == event1["user_id"]
        assert row["channel"] == event1["channel"]
        assert row["text"] == event1["text"]
        assert row["correlation_id"] == correlation_id

        count = await db_conn.fetchval(
            """
            SELECT COUNT(*) 
            FROM notifications
            WHERE correlation_id = $1
            """,
            correlation_id,
        )

        assert count == 1


class TestValidation:
    async def test_invalid_notification_is_not_persisted(self, db_conn):
        response = post_notify({
            "user_id": 1,
            "channel": "#alerts",
            # "text" is missing on purpose
        })

        assert response.status_code == 422

        await assert_notification_count_after_delay(db_conn, 0)