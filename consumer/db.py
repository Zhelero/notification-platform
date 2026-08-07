import asyncpg
from consumer.config import DB_CONFIG


class DuplicateNotificationError(Exception):
    """Raised when a notification with this correlation_id was already persisted."""

    def __init__(self, correlation_id):
        self.correlation_id = correlation_id
        super().__init__(f"Duplicate correlation_id: {correlation_id}")

async def get_pool():
    return await asyncpg.create_pool(
        **DB_CONFIG,
        ssl=False,
    )

async def save_notification(pool, event):
    try:
        await pool.execute(
            """
            INSERT INTO notifications (
                correlation_id,
                user_id,
                channel,
                text
            ) 
            VALUES ($1, $2, $3, $4)
            """,
            event["correlation_id"],
            event["user_id"],
            event["channel"],
            event["text"],
        )
    except asyncpg.exceptions.UniqueViolationError as exc:
        raise DuplicateNotificationError(event["correlation_id"]) from exc