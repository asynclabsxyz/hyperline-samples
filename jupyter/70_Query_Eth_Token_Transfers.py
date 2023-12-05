# name: Batch-read ethereum token transfers
# level: beginner
# engine: spark pyspark
# tags: ethereum
 
import hyperline


# Build/Create spark session
spark = hyperline.get_spark_session(app_name='hyperline-sparksql-sample')


table_name = "hyperlake.hyperdata_stage.tx_calls"
read_data = spark.read.table(table_name)   
read_data.show(truncate=False)
