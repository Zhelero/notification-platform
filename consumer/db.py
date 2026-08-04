import asyncpg
from consumer.config import DB_CONFIG


async def get_pool():
    return await asyncpg.create_pool(
        **DB_CONFIG,
        ssl=False,
    )

async def save_notification(pool, event):
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