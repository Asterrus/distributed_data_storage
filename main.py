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
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dotenv import load_dotenv

from db.engine import create_engine, get_database_url
from iceberg.connect import create_iceberg_table, get_iceberg_catalog
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
    iceberg_catalog = get_iceberg_catalog(
        os.environ["MINIO_ROOT_USER"], os.environ["MINIO_ROOT_PASSWORD"]
    )
    print(f"{iceberg_catalog =}")

    iceberg_table = create_iceberg_table(iceberg_catalog, "logs_table")
    print(f"{iceberg_table =}")

    table = iceberg_catalog.load_table(("default", "logs_table"))
    print(f"{table=}")
    arrow_table = pa.Table.from_pandas(df)
    # downcast timestamp
    arrow_table = arrow_table.set_column(
        arrow_table.schema.get_field_index("timestamp"),
        "timestamp",
        pc.cast(arrow_table["timestamp"], pa.timestamp("us", tz="UTC")),
    )
    for col in ["user_id", "response_time", "status_code"]:
        arrow_table = arrow_table.set_column(
            arrow_table.schema.get_field_index(col),
            col,
            pc.cast(arrow_table[col], pa.int32()),
        )
    table.append(arrow_table)


if __name__ == "__main__":
    main()
