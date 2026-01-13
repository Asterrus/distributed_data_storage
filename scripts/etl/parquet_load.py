import logging

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


def write_parquet(table, where, filesystem):
    """Записывает PyArrow таблицу в Parquet файл на S3."""
    logger.info(f"Запись данных в Parquet файл: {where}")
    pq.write_table(
        table,
        where=where,
        filesystem=filesystem,
    )
    logger.info(f"Успешно записано {len(table)} строк в Parquet")
