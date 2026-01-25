import argparse
import asyncio
import logging

from dotenv import load_dotenv

from db.engine import create_engine, get_database_url
from scripts.insert_logs import insert_logs
from scripts.logs_generation import generate_logs_with_current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()


async def generate_and_write_logs(engine, batch_size):
    await insert_logs(engine, list(generate_logs_with_current_timestamp(batch_size)))
    logger.info(f"Записано {batch_size} логов")


async def main(batch_size: int, interval: int):
    logger.info("Запуск скрипта генерации логов")
    logger.info(f"Количество логов: {batch_size}")
    logger.info(f"Интервал (сек): {interval}")
    engine = create_engine(get_database_url())

    while True:
        await generate_and_write_logs(engine, batch_size)
        await asyncio.sleep(interval)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("batch_size", type=int, nargs="?", default=10)
    parser.add_argument("interval", type=int, nargs="?", default=5)
    args = parser.parse_args()

    try:
        asyncio.run(main(args.batch_size, args.interval))
    except KeyboardInterrupt:
        logger.info("Скрипт генерации остановлен")
