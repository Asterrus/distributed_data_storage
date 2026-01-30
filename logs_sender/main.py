import asyncio
import logging
import os

from db_connection import create_engine, get_database_url
from dotenv import load_dotenv
from scripts.insert_logs import insert_logs
from scripts.logs_generation import generate_logs_with_current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()


async def main():
    batch_size = int(os.environ.get("LOGS_SENDER_BATCH_SIZE", 10))
    interval = int(os.environ.get("LOGS_SENDER_INTERVAL_SEC", 5))

    logger.info("Запуск скрипта генерации логов")
    logger.info(f"Количество логов: {batch_size}")
    logger.info(f"Интервал (сек): {interval}")

    engine = create_engine(get_database_url())

    while True:
        logs = generate_logs_with_current_timestamp(batch_size)

        await insert_logs(engine, list(logs))

        logger.info(f"Записано {batch_size} логов")

        await asyncio.sleep(interval)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Скрипт генерации остановлен")
