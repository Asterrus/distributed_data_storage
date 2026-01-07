import asyncio

from dotenv import load_dotenv

from db.engine import create_engine, get_database_url
from scripts.insert_logs import insert_logs
from scripts.logs_generation import generate_logs

load_dotenv()


def main():
    print(f"{get_database_url() =}")
    engine = create_engine(get_database_url())
    asyncio.run(insert_logs(engine, list(generate_logs(10000))))


if __name__ == "__main__":
    main()
