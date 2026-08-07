import uuid
from unittest.mock import AsyncMock

import asyncpg
import pytest

from consumer.db import DuplicateNotificationError, save_notification


@pytest.fixture
def event():
    return {
        "correlation_id": str(uuid.uuid4()),
        "user_id": 42,
        "channel": "#test",
        "text": "Hello",
    }


async def test_save_notification_inserts_event_fields(event):
    pool = AsyncMock()

    await save_notification(pool, event)

    pool.execute.assert_awaited_once()
    query_args = pool.execute.call_args.args[1:]
    assert query_args == (
        event["correlation_id"],
        event["user_id"],
        event["channel"],
        event["text"],
    )


async def test_save_notification_raises_duplicate_error_on_unique_violation(event):
    pool = AsyncMock()
    pool.execute.side_effect = asyncpg.exceptions.UniqueViolationError(
        "duplicate key value violates unique constraint"
    )

    with pytest.raises(DuplicateNotificationError) as exc_info:
        await save_notification(pool, event)

    assert exc_info.value.correlation_id == event["correlation_id"]


async def test_save_notification_propagates_other_db_errors(event):
    pool = AsyncMock()
    pool.execute.side_effect = asyncpg.exceptions.ConnectionDoesNotExistError(
        "connection was closed"
    )

    with pytest.raises(asyncpg.exceptions.ConnectionDoesNotExistError):
        await save_notification(pool, event)