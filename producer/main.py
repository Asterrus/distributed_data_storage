import asyncio
import logging
import os
from datetime import datetime

from db_connection import create_engine, get_database_url
from dotenv import load_dotenv
from faststream import FastStream
from faststream.kafka import KafkaBroker
from last_timestamp_utils import read_last_ts, validate_state_file, write_last_ts
from read_logs_script import read_recent_logs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

broker = KafkaBroker("kafka:9092")
app = FastStream(broker)
PRODUCER_INTERVAL_SEC = int(os.environ.get("PRODUCER_INTERVAL_SEC") or "5")
KAFKA_TOPIC_NAME = os.environ.get("KAFKA_TOPIC_NAME")

logger.info(f"{PRODUCER_INTERVAL_SEC =}")


async def publish_logs(logs: list[dict]) -> bool:
    """Публикация логов в Kafka"""
    logger.info(f"Публикация {len(logs)} логов в {KAFKA_TOPIC_NAME} топике Kafka")
    try:
        for log in logs:
            await broker.publish(
                log,
                topic=KAFKA_TOPIC_NAME,
            )

        return True
    except Exception as e:
        logger.error(f"Ошибка при публикации логов в Kafka: {e}")
        return False


def validate_logs(logs: list[dict]) -> list[dict]:
    """Валидируем логи перед отправкой. Добавляем event_id и нормализуем timestamp"""
    for log in logs:
        ts = log["timestamp"]
        if isinstance(ts, datetime):
            log["timestamp"] = ts.isoformat()
    return logs


@app.after_startup
async def main():
    logger.info("MAIN PRODUCER")
    engine = create_engine(get_database_url())

    await validate_state_file()

    while True:
        last_ts = await read_last_ts()
        logger.info(f"{last_ts =}")

        logs = await read_recent_logs(engine, last_ts)

        if logs:
            logs = validate_logs(logs)
            published = await publish_logs(logs)
            if published:
                logger.info(f"{len(logs)} логов отправлено")
                await write_last_ts(logs[-1]["timestamp"])

        await asyncio.sleep(PRODUCER_INTERVAL_SEC)
