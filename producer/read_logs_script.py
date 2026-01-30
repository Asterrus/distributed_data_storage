from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


async def read_recent_logs(engine: AsyncEngine, last_ts: str):
    q = """
        SELECT
          timestamp,
          user_id,
          url,
          response_time,
          status_code
        FROM web_logs
        WHERE timestamp > :last_ts
        ORDER BY timestamp
    """
    async with engine.connect() as conn:
        result = await conn.execute(text(q), {"last_ts": last_ts})
        rows = result.fetchall()

    return [dict(row._mapping) for row in rows]
