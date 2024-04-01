"""
Primitives to create Pandas Dataframes represending flipside datasets using direct Snowflake connection.
"""

import snowflake.connector
import pandas as pd
import os

__all__ = [
    "execute_flipside_sql",
]


def execute_flipside_sql(
    query: str,
) -> pd.DataFrame:

    user = os.getenv("FLIPSIDE_USER")
    password = os.getenv("FLIPSIDE_PASSWORD")
    account = "ZSNIARY-FLIPSIDE_PRO"
    warehouse = "PRO"

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
    )
    cur = conn.cursor()

    cur.execute(query)
    return cur.fetch_pandas_all()
