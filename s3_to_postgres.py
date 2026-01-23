import logging
import os
from datetime import datetime, timezone
from time import perf_counter

import polars as pl
from dotenv import load_dotenv
from sqlalchemy import create_engine

from db.engine import get_database_url

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

S3_PATH = "s3a://logs-bucket/parquet/web_logs/web_logs.parquet"

MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
TABLE_NAME = "processed_data_python"


def main():
    engine = create_engine(get_database_url(), pool_pre_ping=True)
    storage_options = {
        "aws_access_key_id": MINIO_ROOT_USER,
        "aws_secret_access_key": MINIO_ROOT_PASSWORD,
        "endpoint_url": "http://localhost:9000",
        "aws_region": "us-east-1",
    }
    t1 = perf_counter()
    df = pl.read_parquet(S3_PATH, storage_options=storage_options)
    t2 = perf_counter()
    logger.info(f"Parquet прочитан: {df.height} строк")
    logger.info(f"Время чтения Parquet: {t2 - t1}")

    t3 = perf_counter()
    df = df.with_columns(processed_at=datetime.now(tz=timezone.utc))
    t4 = perf_counter()
    logger.info("Добавлена колонка 'processed_at'")
    logger.info(f"Время добавления колонки: {t4 - t3}")

    t5 = perf_counter()
    df.write_database(
        table_name=TABLE_NAME,
        connection=engine,
        if_table_exists="append",
        engine="sqlalchemy",
    )
    t6 = perf_counter()

    logger.info(f"Данные записаны в PostgreSQL таблицу '{TABLE_NAME}'")
    logger.info(f"Время записи в PostgreSQL: {t6 - t5}")
    logger.info(f"Общее время: {t6 - t1}")


if __name__ == "__main__":
    main()
