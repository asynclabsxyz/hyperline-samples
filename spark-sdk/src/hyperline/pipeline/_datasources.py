from datetime import datetime
from hyperline.datasets import load_df
from pyspark.sql import SparkSession, DataFrame
from typing import Optional


__all__ = [
    "get_datasource",
]


class DataSource:
    def load_df(
        self,
        table_name: str,
        begin_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> DataFrame:
        pass


class _BigQueryDataSource(DataSource):
    def __init__(self, spark: SparkSession, sample_reads: bool = False):
        self.spark = spark
        self.sample_reads = sample_reads

    def load_df(
        self,
        table_name: str,
        begin_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        limit = None
        if self.sample_reads:
            limit = 5000

        return load_df(self.spark, table_name, begin_time, end_time, limit)


_datasources = {
    "bigquery": _BigQueryDataSource,
}


def get_datasource(
    name: str, spark: SparkSession, sample_reads: bool = False
) -> DataSource:
    return _datasources[name](spark, sample_reads)
