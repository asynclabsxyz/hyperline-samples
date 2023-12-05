"""
Primitives to create Spark Dataframes represending crypto datasets in BigQuery.
"""

from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame

__all__ = [
    "load_df",
    "eth_contracts",
    "eth_tokens",
    "eth_token_transfers",
    "eth_balances",
    "eth_blocks",
    "eth_logs",
    "eth_sessions",
    "eth_traces",
    "eth_transactions",
    "eth_transactions_ens",
    "eth_historic_balances",
    "eth_contract_ens",
    "nft_blur_ethereum",
    "nft_seaport_ethereum",
    "nft_wyvern_ethereum",
]


def load_df(
    spark: SparkSession,
    table_name: str,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, table_name),
        begin=begin_time,
        end=end_time,
        limit=limit,
    )


def eth_contracts(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.contracts"),
        begin=begin_time,
        end=end_time,
    )


def eth_tokens(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.tokens"),
        begin=begin_time,
        end=end_time,
    )


def eth_token_transfers(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.token_transfers"),
        begin=begin_time,
        end=end_time,
    )


def eth_balances(spark: SparkSession) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.balances"),
        begin=None,
        end=None,
    )


def eth_blocks(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.blocks"),
        begin=begin_time,
        end=end_time,
    )


def eth_logs(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.logs"),
        begin=begin_time,
        end=end_time,
    )


def eth_sessions(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.sessions"),
        begin=None,
        end=None,
    )


def eth_traces(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.traces"),
        begin=begin_time,
        end=end_time,
    )


def eth_transactions(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "bigquery-public-data.crypto_ethereum.transactions"),
        begin=begin_time,
        end=end_time,
    )


def eth_historic_balances(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    # TODO: change to permanent dataset once finalized
    return _with_time_range(
        _load_table(spark, "mvp-infra.ethereum.historic_balances_20230113"),
        begin=begin_time,
        end=end_time,
    )


def eth_contract_ens(spark: SparkSession) -> DataFrame:
    return _load_table(spark, "mvp-infra.ethereum.ens")


def nft_blur_ethereum(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "mvp-infra.ethereum.nft_blur_ethereum"),
        begin=begin_time,
        end=end_time,
    )


def nft_seaport_ethereum(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "mvp-infra.ethereum.nft_seaport_ethereum"),
        begin=begin_time,
        end=end_time,
    )


def nft_wyvern_ethereum(
    spark: SparkSession,
    begin_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> DataFrame:
    return _with_time_range(
        _load_table(spark, "mvp-infra.ethereum.nft_wyvern_ethereum"),
        begin=begin_time,
        end=end_time,
    )


def _with_time_range(
    df: DataFrame,
    begin: Optional[datetime] = None,
    end: Optional[datetime] = None,
    limit: int = None,
) -> DataFrame:
    if limit is not None:
        df = df.limit(limit)
    for ts_col in ["timestamp", "block_timestamp"]:
        if ts_col in dict(df.dtypes):
            break
    else:
        # partition column cannot be determined, assume unpartitioned table
        return df

    if not begin and not end:
        raise ValueError(
            "Either begin or end datetime must be provided for Dataframe filter"
        )

    if begin and end:
        return df.where((begin <= df[ts_col]) & (df[ts_col] < end))
    if begin:
        return df.where(begin <= df[ts_col])
    return df.where(df[ts_col] < end)


def _load_table(spark: SparkSession, fqtn: str) -> DataFrame:
    return spark.read.format("bigquery").option("table", fqtn).load()


def eth_transactions_ens(
    spark: SparkSession, ens_name: str, begin_time: Optional[datetime] = None
):
    begin_time_str = "2010-01-01"
    if begin_time is not None:
        begin_time_str = begin_time.strftime("%Y-%m-%d")

    sql = """
      SELECT ens.name, transactions.* FROM `bigquery-public-data.crypto_ethereum.transactions` transactions
      INNER JOIN `mvp-infra.ethereum.ens` ens on transactions.from_address = ens.latest_owner
      WHERE DATE(block_timestamp) > "{0}" 
      AND ens.name='{1}' 
      UNION DISTINCT (
        SELECT ens.name, transactions.* FROM `bigquery-public-data.crypto_ethereum.transactions` transactions
        INNER JOIN `mvp-infra.ethereum.ens` ens on transactions.to_address = ens.latest_owner
        WHERE DATE(block_timestamp) > "{0}"  
        AND ens.name='{1}'
      )
  """
    sql = sql.format(begin_time_str, ens_name)

    tempLocation = "spark_testing"

    # load the result of a SQL query on BigQuery into a DataFrame
    df = (
        spark.read.format("bigquery")
        .option("viewsEnabled", "true")
        .option("materializationDataset", tempLocation)
        .option("query", sql)
        .load()
    )
    return df
