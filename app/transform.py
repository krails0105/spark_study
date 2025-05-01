from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, BooleanType, ArrayType
from pyspark.sql.functions import col, when, size, flatten, expr, from_unixtime, timestamp_seconds, current_timestamp
from pyspark.sql import functions as F


def extract_json(df, json_schema):
    df_parsed = df.withColumn("json_array", F.from_json(F.col("value"), ArrayType(StringType())))
    return df_parsed.withColumn("block", F.from_json(F.col("json_array")[1], json_schema))\
    .select("block.*")\
    .drop("value")\
    .drop("json_array")\


def block_transform(df):
    return df.select(
        col("height").alias("block_height"),
        timestamp_seconds(col("time")).alias("block_time"),
        col("time").alias("block_timestamp"),
        col("hash").alias("block_hash"),
        col("merkleroot").alias("merkle_root"),
        col("chainwork").alias("chain_work"),
        col("tx")[0]["vin"][0]["coinbase"].alias("coinbase_script"),
        col("tx")[0]["txid"].alias("coinbase_transaction_hash"),
        col("bits"),
        col("size"),
        col("strippedsize").alias("stripped_size"),
        col("weight"),
        col("version"),
        col("difficulty"),
        expr("aggregate(filter(tx.fee, x -> x IS NOT NULL), CAST(0 AS DECIMAL(16,8)), (acc, x) -> CAST(acc + x AS DECIMAL(16, 8)))").alias("fee_reward"),
        expr("aggregate(tx[0].vout.value, CAST(0 AS DECIMAL(16,8)), (acc, x) -> CAST(acc + x AS DECIMAL(16, 8)))").alias("block_reward"),
        col("nTx").alias("transaction_count"),
        expr("""
            size(
            flatten(
            transform(
            tx, x -> filter(x.vin, vin -> vin.coinbase IS NULL))
            )
            )
            """).alias("spent_utxo_count"),
        expr("""
        aggregate(
        filter(
            flatten(transform(tx, x -> x.vin)), 
            vin -> vin.coinbase IS NULL
        ), 
        CAST(0 AS DECIMAL(16, 8)), 
        (acc, vin) -> CAST(acc + vin.prevout.value AS DECIMAL(16, 8))
        )
        """).alias("spent_utxo_total"),
        expr("""
            size(
            flatten(
            transform(
            tx,
            x -> filter(x.vout, v -> coalesce(v.value, 0) > 0)
            )
            )
            )
            """).alias("created_utxo_count"),
        expr("""
            aggregate(
            flatten(transform(tx, x -> x.vout)),
            CAST(0 AS DECIMAL(38, 18)), 
            (acc, v) -> CAST(acc + v.value AS DECIMAL(38, 18))
            )
            """).alias("created_utxo_total"),
        col("nonce").alias("nonce"),
        col("previousblockhash").alias("previous_block_hash"),
        col("nextblockhash").alias("next_block_hash"),
        current_timestamp().alias("updated_at")
    ).drop("tx")
 
def transaction_transform(df):
        
    return df.select(
        col("height").alias("block_height"),
        timestamp_seconds(col("time")).alias("block_time"),
        col("time").alias("block_timestamp"),
        col("hash").alias("block_hash"),
        posexplode("tx").alias("transaction_index", "tx"))\
        .select( "block_height", "block_time", "block_timestamp", "block_hash", "transaction_index",
        col("tx.txid").alias("transaction_hash"),
        (col("transaction_index") == 0).alias("is_coinbase"),
        col("tx.size").alias("size"),
        col("tx.vsize").alias("virtual_size"),
        col("tx.weight").alias("weight"),
        col("tx.version").alias("version"),
        col("tx.locktime").alias("lock_time"),
        size("tx.vin").alias("spent_utxo_count"),
        size("tx.vout").alias("created_utxo_count"),
        expr("""
            aggregate(
            transform(tx.vin, v -> coalesce(v.prevout.value, 0)), 
            CAST(0 AS DECIMAL(16, 8)), 
            (acc, v) -> CAST(acc + v AS DECIMAL(16, 8))
            )
        """).alias("spent_utxo_total"),
        expr("""
            aggregate(
                transform(tx.vout, v -> v.value),
                cast(0 as decimal(16,8)),
                (acc, v) -> CAST(acc + v AS DECIMAL(16, 8))
            )
        """).alias("created_utxo_total"),
        col("tx.fee").alias("fee"),
        col("tx.hex").alias("hex"),
        current_timestamp().alias("updated_at")
    )   
    
def transaction_input_transform(df):
    return df.select(
        col("height").alias("block_height"),
        timestamp_seconds(col("time")).alias("block_time"),
        col("time").alias("block_timestamp"),
        col("hash").alias("block_hash"),
        posexplode("tx").alias("transaction_index", "tx"))\
        .select("block_height", "block_time", "block_timestamp", "block_hash", "transaction_index",
        col("tx.txid").alias("transaction_hash"),
        posexplode("tx.vin").alias("vin_index", "vin"))\
        .select("block_height", "block_time", "block_timestamp", "block_hash", "transaction_index", "transaction_hash",    
        col("vin.prevout.value").alias("value"),
        col("vin.prevout.scriptPubKey.address").alias("address"),
        col("vin.prevout.scriptPubKey.type").alias("address_type"),
        col("vin.txid").alias("prevout_transaction_hash"),
        col("vin.vout").cast("int").alias("prevout_transaction_index"),
        col("vin.prevout.height").cast("int").alias("prevout_block_height"),
        col("vin.coinbase").alias("coinbase"),
        (col("transaction_index") == 0).alias("is_coinbase"),
        col("vin.txinwitness").alias("witness"),
        col("vin.scriptSig.asm").alias("script_sig_asm"),
        col("vin.scriptSig.hex").alias("script_sig_hex"),
        col("vin.prevout.scriptPubKey.desc").alias("prevout_script_pubkey_desc"),
        col("vin.prevout.scriptPubKey.asm").alias("prevout_script_pubkey_asm"),
        col("vin.prevout.scriptPubKey.hex").alias("prevout_script_pubkey_hex"),
        col("vin.sequence").cast("long").alias("sequence"),
            current_timestamp().alias("updated_at")
        )

def transaction_output_transform(df):
    return df.select(
    col("height").alias("block_height"),
    timestamp_seconds(col("time")).alias("block_time"),
    col("time").alias("block_timestamp"),
    col("hash").alias("block_hash"),
    posexplode("tx").alias("transaction_index", "tx"))\
    .select("block_height", "block_time", "block_timestamp", "block_hash", "transaction_index",
    col("tx.txid").alias("transaction_hash"),
    posexplode("tx.vout").alias("vout_index", "vout"))\
    .select("block_height", "block_time", "block_timestamp", "block_hash", "transaction_index", "transaction_hash",    
    col("vout.value").alias("value"),
    col("vout.scriptPubKey.address").alias("address"),
    col("vout.scriptPubKey.type").alias("address_type"),
    col("vout.scriptPubKey.asm").alias("script_pubkey_asm"),
    col("vout.scriptPubKey.desc").alias("script_pubkey_desc"),
    col("vout.scriptPubKey.hex").alias("script_pubkey_hex"),
    current_timestamp().alias("updated_at")
    )
