from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, window, avg, count, when
from pyspark.sql.types import StructType, StructField, DoubleType, LongType

KAFKA_SERVERS = "kafka:29092"
TOPICS = "btc_trades,eth_trades"
WATERMARK = "10 seconds"   # how late a trade may arrive and still be counted
WINDOW = "1 second"        # one output row per second

schema = StructType([
    StructField("price", DoubleType()),
    StructField("quantity", DoubleType()),
    StructField("trade_time_ms", LongType()),
])


def parse_trades(raw: DataFrame) -> DataFrame:
    """Kafka rows -> typed trades. Bad messages are dropped instead of becoming NULL rows."""
    return (
        raw.select(col("topic"), from_json(col("value").cast("string"), schema).alias("d"))
        .select("topic", "d.*")
        .filter(col("price").isNotNull() & (col("price") > 0) & col("trade_time_ms").isNotNull())
        # dividing by 1000 and casting keeps millisecond precision (from_unixtime truncates to seconds)
        .withColumn("event_time", (col("trade_time_ms") / 1000).cast("timestamp"))
    )


def ratio_per_window(trades: DataFrame) -> DataFrame:
    """One windowed aggregation over both symbols -> one row per second with both prices."""
    is_btc = col("topic") == "btc_trades"
    is_eth = col("topic") == "eth_trades"
    return (
        trades.withWatermark("event_time", WATERMARK)
        .groupBy(window("event_time", WINDOW))
        .agg(
            avg(when(is_btc, col("price"))).alias("btc_price"),
            avg(when(is_eth, col("price"))).alias("eth_price"),
            count(when(is_btc, 1)).alias("btc_trades"),
            count(when(is_eth, 1)).alias("eth_trades"),
        )
        # keep only seconds where both symbols traded
        .filter(col("btc_price").isNotNull() & col("eth_price").isNotNull())
        .withColumn("btc_time", col("window.start"))
        .withColumn("btc_eth_ratio", col("btc_price") / col("eth_price"))
        .select("btc_time", "btc_price", "eth_price", "btc_eth_ratio", "btc_trades", "eth_trades")
    )


def main():
    spark = (
        SparkSession.builder.appName("CryptoRatioWindow")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.shuffle.partitions", "4")  # default 200 is wasteful for a local demo
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPICS)
        .load()
    )
    final_stream = ratio_per_window(parse_trades(raw))

    print("Writing windowed BTC/ETH ratio to the Data Lake...")
    query = (
        final_stream.writeStream.outputMode("append")
        .format("parquet")
        .option("path", "/app/data/output/crypto_ratio")
        .option("checkpointLocation", "/app/data/checkpoints_ratio")
        .trigger(processingTime="10 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
