import logging
import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    from_json,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

schema = ArrayType(
    StructType(
        [
            StructField("timestamp", StringType()),
            StructField("user_id", IntegerType()),
            StructField("url", StringType()),
            StructField("response_time", IntegerType()),
            StructField("status_code", IntegerType()),
        ]
    )
)
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
SPARK_MASTER_PORT = os.getenv("SPARK_MASTER_PORT")
POSTGRES_JDBC_VERSION = os.getenv("POSTGRES_JDBC_VERSION")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
TABLE_NAME = "processed_data"
JDBC_DRIVER = "org.postgresql.Driver"
JDBC_JAR = f"jars/postgresql-{POSTGRES_JDBC_VERSION}.jar"
assert all(
    [DB_HOST, DB_PORT, SPARK_MASTER_PORT, POSTGRES_JDBC_VERSION, KAFKA_TOPIC_NAME]
)


def write_to_postgres(batch_df: DataFrame, batch_id: int):
    logger.info(f"Inserting {batch_df.count()} rows to {TABLE_NAME}")
    rows = batch_df.select(
        "event_id",
        "user_id",
        "url",
        "response_time",
        "status_code",
        "timestamp",
        "processed_at",
    ).collect()

    if not rows:
        return
    data = [
        (
            row.event_id,
            row.user_id,
            row.url,
            row.response_time,
            row.status_code,
            row.timestamp,
            row.processed_at,
        )
        for row in rows
    ]
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    try:
        execute_values(
            cur,
            """
            INSERT INTO processed_data (
                event_id, user_id, url, response_time,
                status_code, timestamp, processed_at
            ) VALUES %s
            ON CONFLICT (event_id) DO NOTHING
            """,
            data,
            page_size=500,
        )
        conn.commit()
        logger.info(
            f"Batch {batch_id}: inserted {cur.rowcount} new rows (skipped {len(data) - cur.rowcount} duplicates)"
        )
    except Exception as e:
        conn.rollback()
        logger.error(f"Batch {batch_id} failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def main():
    spark = (
        SparkSession.builder.appName("Kafka2Postgres")
        .master(f"spark://spark-master:{SPARK_MASTER_PORT}")
        .config("spark.jars", JDBC_JAR)
        .config("spark.sql.streaming.ui.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", KAFKA_TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    raw_df = kafka_df.selectExpr("CAST(value AS STRING) as json")

    parsed_df = (
        raw_df.select(from_json(col("json"), schema).alias("data"))
        .filter(col("data").isNotNull())
        .select(explode(col("data")).alias("row"))
        .select("row.*")
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("processed_at", current_timestamp())
        .withColumn(
            "event_id",
            F.sha1(
                F.concat_ws(
                    "|",
                    F.col("user_id"),
                    F.col("url"),
                    F.col("response_time").cast("string"),
                    F.col("status_code").cast("string"),
                    F.col("timestamp").cast("string"),
                )
            ),
        )
    )

    parsed_df.writeStream.foreachBatch(write_to_postgres).option(
        "checkpointLocation", "/opt/spark/checkpoints/kafka_to_pg"
    ).outputMode("append").start()

    logger.info("Spark stream started")

    spark.streams.awaitAnyTermination()

    logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
