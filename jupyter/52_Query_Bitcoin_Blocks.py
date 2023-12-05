# name: Batch-read bitcoin blocks
# level: beginner
# engine: spark pyspark
# tags: ethereum
 
import hyperline
from datetime import datetime


# Build/Create spark session
spark = hyperline.get_spark_session(app_name='hyperline-sparksql-sample')

#Load data
blocks_data = hyperline.datasets.bitcoin_blocks(
  spark, begin_time=datetime(2023, 6, 1), end_time=datetime(2023, 6, 2))
    
# Create a table view for the data
blocks_data.createOrReplaceTempView('blocks')

# Run SQL query over data
blocks = spark.sql("SELECT * FROM blocks")

# Preview results
blocks.show()
