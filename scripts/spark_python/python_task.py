import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

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
print(f'{MINIO_ROOT_USER =} {MINIO_ROOT_PASSWORD=}')
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

# PARQUET_PATH = "data/web_logs.parquet"
TABLE_NAME = "processed_data"
JDBC_DRIVER = "org.postgresql.Driver"
JDBC_JAR = f"jars/postgresql-{POSTGRES_JDBC_VERSION}.jar"
# S3_PATH = "s3a://logs-bucket/parquet/web_logs"
S3_PATH = "s3a://logs-bucket/parquet/web_logs/web_logs.parquet"
# S3_PATH = "s3a://logs-bucket/iceberg/default/web_logs"

def main():
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

    print("Spark сессия создана")

    df = spark.read.parquet(S3_PATH)

    print(f"Parquet прочитан: {df.count()} строк")

    df = df.withColumn("processed_at", current_timestamp())

    print("Добавлена колонка 'processed_at'")

    jdbc_url = f"jdbc:postgresql://db:5432/{DB_NAME}"
    (
        df.repartition(4)
          .write
          .format("jdbc")
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

    print(f"Данные записаны в PostgreSQL таблицу '{TABLE_NAME}'")
    spark.stop()


if __name__ == "__main__":
    main()
