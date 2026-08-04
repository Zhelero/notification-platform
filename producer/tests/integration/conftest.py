import asyncpg
import pytest_asyncio

@pytest_asyncio.fixture(scope="function")
async def db_conn():
    conn = await asyncpg.connect(
        host="127.0.0.1",
        port=5433,
        user="app",
        password="app",
        database="notifications",
    )

    yield conn

    await conn.close()

@pytest_asyncio.fixture(autouse=True)
async def clean_notifications(db_conn):
    await db_conn.execute("TRUNCATE notifications RESTART IDENTITY;")
    yield