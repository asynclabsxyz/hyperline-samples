# name: Batch-read sui events
# level: beginner
# engine: spark pyspark
# tags: ethereum
 
import hyperline
from datetime import datetime


# Build/Create spark session
spark = hyperline.get_spark_session(app_name='hyperline-sparksql-sample')

#Load data
transactions_data = hyperline.datasets.load_df(
  spark, "mvp-infra.sui.transactions", begin_time=datetime(2023, 1, 1), end_time=datetime(2023, 6, 2))
    
# Create a table view for the data
transactions_data.createOrReplaceTempView('transactions')

# Run SQL query over data
transactions = spark.sql("SELECT * FROM transactions")

# Preview results
transactions.show()
