import os

from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.schema import NestedField, Schema
from pyiceberg.types import IntegerType, StringType, TimestamptzType

schema = Schema(
    NestedField(1, "timestamp", TimestamptzType()),
    NestedField(2, "user_id", IntegerType()),
    NestedField(3, "url", StringType()),
    NestedField(4, "response_time", IntegerType()),
    NestedField(5, "status_code", IntegerType()),
)


def get_iceberg_catalog(key: str, secret: str):
    pg_user = os.getenv("ICEBERG_POSTGRES_USER")
    pg_password = os.getenv("ICEBERG_POSTGRES_PASSWORD")
    pg_db = os.getenv("ICEBERG_POSTGRES_DB")
    pg_host = os.getenv("ICEBERG_POSTGRES_HOST")
    pg_port = os.getenv("ICEBERG_POSTGRES_PORT")

    uri = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"

    catalog = load_catalog(
        name="local",
        **{
            "type": "sql",
            "uri": uri,
            "warehouse": "s3://logs-bucket/iceberg",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": key,
            "s3.secret-access-key": secret,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )
    return catalog


def create_iceberg_table(catalog: Catalog, identifier):
    namespace = "default"
    catalog.create_namespace_if_not_exists(namespace)
    table = catalog.create_table_if_not_exists(
        identifier=(namespace, identifier),
        schema=schema,
    )
    return table
