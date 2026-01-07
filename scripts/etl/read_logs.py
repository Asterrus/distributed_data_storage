import pandas as pd
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


async def read_logs(engine: AsyncEngine) -> pd.DataFrame:
    q = """
        SELECT
          timestamp,
          user_id,
          url,
          response_time,
          status_code
        FROM web_logs
    """
    async with engine.connect() as conn:
        result = await conn.execute(text(q))
        rows = result.fetchall()

    df = pd.DataFrame(rows)
    return df
