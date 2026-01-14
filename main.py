import asyncio
import logging
import os

import pandas as pd
import pyarrow as pa
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import AsyncEngine

from db.engine import create_engine, get_database_url
from iceberg.connect import create_iceberg_table, get_iceberg_catalog
from s3.connect import get_s3_connection
from scripts.etl.parquet_load import write_parquet
from scripts.etl.read_logs import read_logs
from scripts.insert_logs import insert_logs
from scripts.logs_generation import generate_logs

load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def generate_and_write_data_to_db(engine):
    logger.info("Генерация и загрузка логов в PostgreSQL...")
    await insert_logs(engine, list(generate_logs(10000)))
    logger.info("Логи загружены в PostgreSQL\n")


async def load_data_from_db(engine: AsyncEngine) -> pd.DataFrame:
    logger.info("Чтение логов из PostgreSQL...")
    data = await read_logs(engine)
    logger.info(f"Прочитано {len(data):,} строк\n")
    return data


def get_s3_conn(munio_user: str, minio_password: str):
    logger.info("🔌 Подключение к S3...")
    s3_connection = get_s3_connection(key=munio_user, secret=minio_password)
    logger.info("S3 соединение получено\n")
    return s3_connection


def validate_df(df: pd.DataFrame):
    """Конвертирует типы DataFrame для совместимости с Iceberg."""
    logger.info("Валидация данных...")
    df["timestamp"] = df["timestamp"].dt.as_unit("us")
    for col in ["user_id", "response_time", "status_code"]:
        df[col] = df[col].astype("int32")
    logger.info("Валидация данных завершена")


def save_parquet(s3_connection, df: pd.DataFrame):
    logger.info("Сохранение данных в Parquet формат...")

    table = pa.Table.from_pandas(df)

    write_parquet(
        table=table,
        where="logs-bucket/parquet/web_logs/web_logs.parquet",
        filesystem=s3_connection,
    )

    logger.info("Данные сохранены в Parquet\n")


def save_iceberg(munio_user: str, minio_password: str, data: pd.DataFrame):
    """Загружает данные в Iceberg таблицу."""
    logger.info("Создание и загрузка данных в Iceberg таблицу...")
    iceberg_catalog = get_iceberg_catalog(munio_user, minio_password)

    create_iceberg_table(iceberg_catalog, "web_logs")

    table = iceberg_catalog.load_table(("default", "web_logs"))

    arrow_table = pa.Table.from_pandas(data)

    table.append(arrow_table)
    logger.info("Данные загружены в Iceberg\n")


async def main():
    logger.info("Начало работы скрипта...\n")

    munio_user = os.environ["MINIO_ROOT_USER"]
    minio_password = os.environ["MINIO_ROOT_PASSWORD"]

    # 1. Создаем подключение к БД
    engine = create_engine(get_database_url())

    # 2. Генерируем и загружаем данные в БД
    await generate_and_write_data_to_db(engine)

    logger.info("Начинаем ETL процесс...\n")
    # 3. Читаем данные из БД
    data = await load_data_from_db(engine)

    # 4. Получаем S3 подключение
    s3_connection = get_s3_conn(munio_user, minio_password)

    # 5. Валидируем данные
    validate_df(data)

    # 6. Сохраняем в Parquet формат
    save_parquet(s3_connection, data)

    # 7. Сохраняем в Iceberg формат
    save_iceberg(munio_user, minio_password, data)

    logger.info("ETL процесс успешно завершен!\n")


if __name__ == "__main__":
    asyncio.run(main())
