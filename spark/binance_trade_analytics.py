from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    sum,
    count,
    current_timestamp,
    min as spark_min,
    max as spark_max,
    first,
    last,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
)


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("BTCKlineAnalytics")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    spark = create_spark_session()

    schema = StructType([
        StructField("trade_id", LongType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("event_time_ms", LongType(), True),
        StructField("trade_time_ms", LongType(), True),
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "market-trade-topic")
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    trade_df = (
        parsed_df
        .filter(
            "trade_id is not null and symbol is not null and "
            "price is not null and quantity is not null and trade_time_ms is not null"
        )
        .withColumn("trade_time", to_timestamp((col("trade_time_ms") / 1000).cast("double")))
    )

    # 真的 K 線 OHLC
    kline_df = (
        trade_df
        .withWatermark("trade_time", "10 seconds")
        .groupBy(
            window(col("trade_time"), "5 seconds"),
            col("symbol")
        )
        .agg(
            first("price").alias("open_price"),
            spark_max("price").alias("high_price"),
            spark_min("price").alias("low_price"),
            last("price").alias("close_price"),
            sum("quantity").alias("total_volume"),
            count("*").alias("trade_count")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("open_price"),
            col("high_price"),
            col("low_price"),
            col("close_price"),
            col("total_volume"),
            col("trade_count")
        )
        .withColumn("processed_at", current_timestamp())
    )

    def write_to_postgres(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            print(f"batch {batch_id}: no data")
            return

        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/postgres") \
            .option("dbtable", "btc_kline_5s") \
            .option("user", "postgres") \
            .option("password", "123456") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"batch {batch_id}: written to btc_kline_5s")

    query = (
        kline_df.writeStream
        .foreachBatch(write_to_postgres)
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", "/tmp/checkpoint_btc_kline_5s")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()