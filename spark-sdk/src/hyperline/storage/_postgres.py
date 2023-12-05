from pyspark.sql import DataFrame, SparkSession
from . import _helper

__all__ = [
    "readDataFrame",
    "writeDataFrame",
]

_jdbcHostname = "10.58.48.3"
_jdbcUrlPrefix = "jdbc:postgresql://" + _jdbcHostname + "/"
_postgresDriver = "org.postgresql.Driver"

def readDataFrame(spark: SparkSession, tablename: str): 
    jdbcUrl = _jdbcUrlPrefix + _helper._databaseName()
    properties = {
        "driver": _postgresDriver,
        "user": _helper._databaseUsername(), 
        "password": _helper._databasePassword()
    }
    return spark.read.jdbc(url=jdbcUrl, table=tablename, properties=properties)

def writeDataFrame(df: DataFrame, tablename: str, mode: str = "overwrite"): 
    jdbcUrl = _jdbcUrlPrefix + _helper._databaseName()
    properties = {
        "driver": _postgresDriver,
        "user": _helper._databaseUsername(), 
        "password": _helper._databasePassword()
    }
    df.write.jdbc(url=jdbcUrl, table=tablename, mode=mode, properties=properties)
