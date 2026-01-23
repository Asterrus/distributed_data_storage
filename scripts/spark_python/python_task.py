import logging
import os
from time import perf_counter

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")


SPARK_MASTER_PORT = os.getenv("SPARK_MASTER_PORT")
POSTGRES_JDBC_VERSION = os.getenv("POSTGRES_JDBC_VERSION")

MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
print(f"{MINIO_ROOT_USER =} {MINIO_ROOT_PASSWORD=}")
assert all(
    [
        DB_HOST,
        DB_PORT,
        DB_NAME,
        DB_USER,
        DB_PASS,
        SPARK_MASTER_PORT,
        POSTGRES_JDBC_VERSION,
    ]
)

TABLE_NAME = "processed_data"
JDBC_DRIVER = "org.postgresql.Driver"
JDBC_JAR = f"jars/postgresql-{POSTGRES_JDBC_VERSION}.jar"
S3_PATH = "s3a://logs-bucket/parquet/web_logs/web_logs.parquet"


def main():
    t1 = perf_counter()
    spark = (
        SparkSession.builder.appName("Parquet2Postgres")
        .master(f"spark://spark-master:{SPARK_MASTER_PORT}")
        .config("spark.jars", JDBC_JAR)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ROOT_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_ROOT_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    logger.info("Spark сессия создана")

    t1 = perf_counter()
    df = spark.read.parquet(S3_PATH)
    t2 = perf_counter()

    logger.info(f"Parquet прочитан: {df.count()} строк")
    logger.info(f"Время чтения Parquet: {t2 - t1}")

    t3 = perf_counter()
    df = df.withColumn("processed_at", current_timestamp())
    t4 = perf_counter()

    logger.info("Добавлена колонка 'processed_at'")
    logger.info(f"Время добавления колонки: {t4 - t3}")

    jdbc_url = f"jdbc:postgresql://db:5432/{DB_NAME}"
    t5 = perf_counter()
    (
        df.repartition(4)
        .write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", TABLE_NAME)
        .option("user", DB_USER)
        .option("password", DB_PASS)
        .option("driver", JDBC_DRIVER)
        .option("batchsize", 10000)
        .option("isolationLevel", "READ_COMMITTED")
        .mode("append")
        .save()
    )
    t6 = perf_counter()

    logger.info(f"Данные записаны в PostgreSQL таблицу '{TABLE_NAME}'")
    logger.info(f"Время записи в PostgreSQL: {t6 - t5}")
    logger.info(f"Общее время: {t6 - t1}")

    spark.stop()


if __name__ == "__main__":
    main()
