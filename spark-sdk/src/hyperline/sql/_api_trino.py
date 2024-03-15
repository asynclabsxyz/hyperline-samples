"""
Primitives to create Pandas Dataframes represending crypto datasets using Hyperline Trino.
"""
from trino.dbapi import connect
import pandas as pd
import os

__all__ = [
    "execute_sql",
    "hello_sql",
]

def hello_sql(query: str) -> str:
  return f"Hello, {query}!"

def execute_sql(
  query: str,
) -> pd.DataFrame:
  
  host = os.getenv('TRINO_IP') 
  workspace = os.getenv('WORKSPACE_NAME')

  port = "8080"
  user = f'ws-{workspace}@mvp-infra.iam.gserviceaccount.com'
  catalog="hyperline"
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


