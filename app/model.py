from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, BooleanType, ArrayType, LongType


# BitcoinRawVinScriptSig Schema
bitcoin_raw_vin_script_sig_schema = StructType([
    StructField("asm", StringType(), True),
    StructField("hex", StringType(), True)
])

# BitcoinRawScriptPubKey Schema
bitcoin_raw_script_pub_key_schema = StructType([
    StructField("asm", StringType(), True),
    StructField("desc", StringType(), True),
    StructField("hex", StringType(), True),
    StructField("type", StringType(), True),
    StructField("address", StringType(), True)
])

# BitcoinRawVinPrevout Schema
bitcoin_raw_vin_prevout_schema = StructType([
    StructField("generated", BooleanType(), True),
    StructField("height", IntegerType(), True),
    StructField("value", DecimalType(16, 8), True),
    StructField("scriptPubKey", bitcoin_raw_script_pub_key_schema, True)
])

# BitcoinRawVin Schema
bitcoin_raw_vin_schema = StructType([
    StructField("coinbase", StringType(), True),
    StructField("txinwitness", ArrayType(StringType()), True),
    StructField("sequence", IntegerType(), True),
    
    StructField("txid", StringType(), True),
    StructField("vout", IntegerType(), True),
    StructField("scriptSig", bitcoin_raw_vin_script_sig_schema, True),
    StructField("prevout", bitcoin_raw_vin_prevout_schema, True),
])

# BitcoinRawVout Schema
bitcoin_raw_vout_schema = StructType([
    StructField("value", DecimalType(16, 8), True),
    StructField("n", IntegerType(), True),
    StructField("scriptPubKey", bitcoin_raw_script_pub_key_schema, True)
])

# BitcoinRawTransaction Schema
bitcoin_raw_transaction_schema = StructType([
    StructField("txid", StringType(), True),
    StructField("hash", StringType(), True),
    StructField("version", IntegerType(), True),
    StructField("size", IntegerType(), True),
    StructField("vsize", IntegerType(), True),
    StructField("weight", IntegerType(), True),
    StructField("locktime", LongType(), True),
    StructField("vin", ArrayType(bitcoin_raw_vin_schema), True),
    StructField("vout", ArrayType(bitcoin_raw_vout_schema), True),
    StructField("fee", DecimalType(16, 8), True),
    StructField("hex", StringType(), True)
])

# BitcoinRawBlock Schema
raw_block_schema = StructType([
    StructField("hash", StringType(), True),
    StructField("confirmations", IntegerType(), True),
    StructField("height", IntegerType(), True),
    StructField("version", IntegerType(), True),
    StructField("versionHex", StringType(), True),
    StructField("merkleroot", StringType(), True),
    StructField("time", IntegerType(), True),
    StructField("mediantime", IntegerType(), True),
    StructField("nonce", LongType(), True),
    StructField("bits", StringType(), True),
    StructField("difficulty", DecimalType(32, 9), True),
    StructField("chainwork", StringType(), True),
    StructField("nTx", IntegerType(), True),
    StructField("previousblockhash", StringType(), True),
    StructField("nextblockhash", StringType(), True),
    StructField("strippedsize", IntegerType(), True),
    StructField("size", IntegerType(), True),
    StructField("weight", IntegerType(), True),
    StructField("tx", ArrayType(bitcoin_raw_transaction_schema), True)
])
