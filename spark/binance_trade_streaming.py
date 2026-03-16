from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp
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
        .appName("BinanceTradeStreamingPipeline")
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

    clean_df = (
        parsed_df
        .filter(
            "trade_id is not null and symbol is not null and "
            "price is not null and quantity is not null and "
            "event_time_ms is not null and trade_time_ms is not null"
        )
        .withColumn("event_time", to_timestamp((col("event_time_ms") / 1000).cast("double")))
        .withColumn("trade_time", to_timestamp((col("trade_time_ms") / 1000).cast("double")))
        .withColumn("processed_at", current_timestamp())
    )

    def write_to_postgres(batch_df, batch_id: int) -> None:
        if batch_df.isEmpty():
            print(f"batch {batch_id}: no data")
            return

        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/postgres") \
            .option("dbtable", "market_trade_clean") \
            .option("user", "postgres") \
            .option("password", "123456") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(f"batch {batch_id}: written to market_trade_clean")

    query = (
        clean_df.writeStream
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", "/tmp/checkpoint_market_trade_clean")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()