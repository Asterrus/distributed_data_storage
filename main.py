# import asyncio

# from dotenv import load_dotenv

# from db.engine import create_engine, get_database_url
# from scripts.insert_logs import insert_logs
# from scripts.logs_generation import generate_logs

# load_dotenv()


# def main():
#     print(f"{get_database_url() =}")
#     engine = create_engine(get_database_url())
#     asyncio.run(insert_logs(engine, list(generate_logs(10000))))


# if __name__ == "__main__":
#     main()

import asyncio
import os

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from db.engine import create_engine, get_database_url
from s3.connect import get_s3_connection
from scripts.etl.parquet_load import write_parquet
from scripts.etl.read_logs import read_logs

load_dotenv()


def main():
    print(f"{get_database_url() =}")
    engine = create_engine(get_database_url())
    # asyncio.run(insert_logs(engine, list(generate_logs(10000))))
    df = asyncio.run(read_logs(engine))
    s3_connection = get_s3_connection(
        key=os.environ["MINIO_ROOT_USER"], secret=os.environ["MINIO_ROOT_PASSWORD"]
    )
    table = pa.Table.from_pandas(df)
    print(s3_connection)
    write_parquet(
        table=table,
        where="logs-bucket/parquet/web_logs/web_logs.parquet",
        filesystem=s3_connection,
    )
    print(
        pq.read_table(
            "logs-bucket/parquet/web_logs/web_logs.parquet",
            filesystem=s3_connection,
        )
        .to_pandas()
        .head()
    )


if __name__ == "__main__":
    main()
