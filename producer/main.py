import asyncio
import logging
import os

from dotenv import load_dotenv
from faststream import FastStream
from faststream.kafka import KafkaBroker

from db_connection import create_engine, get_database_url
from last_timestamp_utils import read_last_ts, validate_state_file, write_last_ts
from read_logs_script import read_recent_logs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

broker = KafkaBroker("kafka:9092")
app = FastStream(broker)
PRODUCER_INTERVAL_SEC = int(os.environ.get("PRODUCER_INTERVAL_SEC") or "5")
logger.info(f"{PRODUCER_INTERVAL_SEC =}")


async def publish_logs(logs: list[dict]):
    logger.info("PUB")
    await broker.publish(
        logs,
        topic="logs_topic",
    )
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
        logger.info(f"{logs =}")

        if logs:
            await publish_logs(logs)

            logger.info(f"{len(logs)} логов отправлено")
            logger.info(logs[0])
            logger.info(logs[-1])
            await write_last_ts(logs[-1]["timestamp"])

        await asyncio.sleep(PRODUCER_INTERVAL_SEC)
