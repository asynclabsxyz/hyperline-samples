# name: Batch-read and join ethereum transactions and contracts
# level: beginner
# engine: spark pyspark
# tags: ethereum
 
import hyperline
from datetime import datetime
import pyspark.sql.functions as ssf


# Build/Create spark session
spark = hyperline.get_spark_session(app_name='hyperline-sparksql-sample')

#Load data
contracts_data = hyperline.datasets.eth_contracts(
  spark, begin_time=datetime(2023, 1, 1), end_time=datetime(2023, 6, 2))
    
transactions_data = hyperline.datasets.eth_transactions(
  spark, begin_time=datetime(2023, 6, 1), end_time=datetime(2023, 6, 2))

# Create a table view for the data
contracts_data.createOrReplaceTempView('contracts')
transactions_data.createOrReplaceTempView('transactions')

# Run SQL query over data
contracts = spark.sql("SELECT * FROM contracts")
transactions = spark.sql("SELECT * FROM transactions")


joined = transactions \
        .join(contracts, contracts.address == transactions.to_address) \
        .drop(ssf.col("address")) 

# Preview results
joined.show()
print("Total Rows: " + str(joined.count()))
