from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, size, flatten, expr, from_unixtime

from datetime import datetime, timezone
from pyspark import StorageLevel
from transform import block_transform, transaction_transform, transaction_input_transform, transaction_output_transform

# JSON 파일 읽기
json_path = "s3://tech-multichain-etl-finalized-prod-s3/btc/block1*.json"
df = spark.read.text(json_path)

df_parsed = df.withColumn("json_array", F.from_json(F.col("value"), ArrayType(StringType())))
df_parsed = df_parsed.withColumn("block", F.from_json(F.col("json_array")[1], raw_block_schema))\
.select("block.*")\
.drop("value")\
.drop("json_array")\
.persist(StorageLevel.MEMORY_AND_DISK)


df_parsed_block = block_transform(df=df_parsed)    
df_parsed_tx = transaction_transform(df=df_parsed)    
df_parsed_tx_in = transaction_input_transform(df=df_parsed)        
df_parsed_tx_out = transaction_output_transform(df=df_parsed)    

df_parsed_block.writeTo("glue_catalog.default.btc_raw_block").append()
df_parsed_tx.writeTo("glue_catalog.default.btc_raw_transaction").append()
df_parsed_tx_in.writeTo("glue_catalog.default.btc_raw_transaction_input").append()
df_parsed_tx_out.writeTo("glue_catalog.default.btc_raw_transaction_output").append()

spark.catalog.clearCache()  # 작업 완료 후 메모리 해제