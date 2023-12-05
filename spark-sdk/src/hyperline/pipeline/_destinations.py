from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType
import traceback


__all__ = [
    "get_destination",
]


class Destination:
    def append(self, df: DataFrame, table_name: str):
        pass

    def overwrite_partitions(self, df: DataFrame, table_name: str):
        pass


class _IcebergDestination(Destination):
    def __init__(self, spark: SparkSession, db_name: str, test_mode: bool = False):
        self.db_name = db_name
        self.spark = spark
        self.test_mode = test_mode
        self._create_db()

    def _create_db(self):
        self.spark.sql("CREATE DATABASE IF NOT EXISTS " + self._full_db_name())

    def _full_db_name(self):
        if self.test_mode:
            return self.db_name + "_test"
        return self.db_name

    def _full_table_name(self, table_name: str):
        return self._full_db_name() + "." + table_name

    def _writer(self, df: DataFrame, table_name: str):
        return df.writeTo(self._full_table_name(table_name)).using("iceberg")

    def append(self, df: DataFrame, table_name: str):
        self._writer(df, table_name).append()

    def create_table(
        self,
        table_name: str,
        schema: StructType,
        partition_col: Column = None,
        *partition_cols: Column
    ):
        try:
            df = self.spark.createDataFrame([], schema)
            dfWriter = self._writer(df, table_name)
            if partition_col is not None:
                dfWriter = dfWriter.partitionedBy(partition_col, *partition_cols)
            dfWriter.create()
        except Exception as e:
            # Creating a table that already exists throws an error.
            # This would catch all errors but that's okay.
            print(traceback.format_exc())

    def overwrite_partitions(self, df: DataFrame, table_name: str):
        self._writer(df, table_name).overwritePartitions()


_destinations = {
    "iceberg": _IcebergDestination,
}


def get_destination(
    name: str, spark: SparkSession, db_name: str, test_mode: bool = False
) -> Destination:
    return _destinations[name](spark, db_name, test_mode)
