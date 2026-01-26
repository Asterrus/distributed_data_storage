from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


async def insert_logs(engine: AsyncEngine, logs: Sequence):
    query = text("""
        INSERT INTO web_logs (
            timestamp,
            user_id,
            url,
            response_time,
            status_code
        ) VALUES (
            :timestamp,
            :user_id,
            :url,
            :response_time,
            :status_code
        )
    """)

    async with engine.begin() as conn:
        await conn.execute(query, logs)
