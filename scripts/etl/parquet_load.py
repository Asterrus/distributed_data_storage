import pyarrow.parquet as pq


def write_parquet(table, where, filesystem):
    pq.write_table(
        table,
        where=where,
        filesystem=filesystem,
    )
