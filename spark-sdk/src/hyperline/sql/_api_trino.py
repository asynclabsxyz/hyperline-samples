"""
Primitives to create Pandas Dataframes represending crypto datasets using Hyperline Trino.
"""
from trino.dbapi import connect
import pandas as pd

__all__ = [
    "execute_sql",
]

def hello_sql(query: str) -> str:
  return f"Hello, {query}!"

def execute_sql(
  query: str,
) -> pd.DataFrame:
  host = '10.138.0.11'
  port = "8080"
  user = 'ws-demo3@mvp-infra.iam.gserviceaccount.com'
  catalog="hyperlake"
  schema="hyperdata"

  conn = connect(
      host=host,
      port=port,
      user=user,
      catalog=catalog,
      schema=schema,
  )
  cur = conn.cursor()

  cur.execute(query)
  rows = cur.fetchall()
  df = pd.DataFrame(rows)
  return df


