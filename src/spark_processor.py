from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, expr
from pyspark.sql.types import StructType, StructField, DoubleType, LongType
from pyspark.sql.functions import from_json, col, from_unixtime, expr, lit

spark = SparkSession.builder \
    .appName("CryptoArbitrageJoin") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("trade_time_ms", LongType())
])

# 1. Read and Parse BTC Stream
btc_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "btc_trades").load()
btc_parsed = btc_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
btc_wm = btc_parsed \
    .withColumn("btc_time", from_unixtime(col("trade_time_ms") / 1000).cast("timestamp")) \
    .withColumnRenamed("price", "btc_price") \
    .withColumn("join_key", lit(1)) \
    .withWatermark("btc_time", "1 minute")

# 2. Read and Parse ETH Stream
eth_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:29092").option("subscribe", "eth_trades").load()
eth_parsed = eth_stream.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
eth_wm = eth_parsed \
    .withColumn("eth_time", from_unixtime(col("trade_time_ms") / 1000).cast("timestamp")) \
    .withColumnRenamed("price", "eth_price") \
    .withColumn("join_key", lit(1)) \
    .withWatermark("eth_time", "1 minute")

# 3. Stream-to-Stream Join (Matches trades occurring within 10 seconds of each other)
btc_aliased = btc_wm.alias("btc")
eth_aliased = eth_wm.alias("eth")

joined_stream = btc_aliased.join(
    eth_aliased,
    expr("""
        btc.join_key = eth.join_key AND
        eth.eth_time >= btc.btc_time - interval 10 seconds AND
        eth.eth_time <= btc.btc_time + interval 10 seconds
    """)
)

# 4. Calculate Ratio and Format Output
final_stream = joined_stream \
    .withColumn("btc_eth_ratio", col("btc_price") / col("eth_price")) \
    .select("btc_time", "btc_price", "eth_price", "btc_eth_ratio")

# 5. Sink to Parquet in Micro-batches
print("Writing Stream-to-Stream Join to Data Lake...")
query = final_stream.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/app/data/output/crypto_ratio") \
    .option("checkpointLocation", "/app/data/checkpoints_ratio") \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()