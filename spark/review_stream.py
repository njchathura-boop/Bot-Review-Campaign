from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F, types as T


REVIEW_SCHEMA = T.StructType(
    [
        T.StructField("review_id", T.StringType(), False),
        T.StructField("user_id", T.StringType(), False),
        T.StructField("product_id", T.StringType(), False),
        T.StructField("text", T.StringType(), False),
        T.StructField("rating", T.DoubleType(), False),
        T.StructField("timestamp", T.LongType(), False),
        T.StructField("verified_purchase", T.BooleanType(), True),
        T.StructField("helpful_votes", T.LongType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("source", T.StringType(), True),
        T.StructField("schema_version", T.IntegerType(), False),
    ]
)


def build_features(frame):
    valid = frame.filter(
        F.col("review_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("product_id").isNotNull()
        & (F.length(F.trim("text")) >= 3)
        & F.col("rating").between(1, 5)
    )
    timestamp_seconds = F.when(F.col("timestamp") > 10_000_000_000, F.col("timestamp") / 1000).otherwise(F.col("timestamp"))
    return (
        valid.withColumn("event_time", F.to_timestamp(F.from_unixtime(timestamp_seconds)))
        .withColumn("review_length", F.length("text"))
        .withColumn("word_count", F.size(F.split(F.trim("text"), r"\s+")))
        .withColumn("exclamation_count", F.length("text") - F.length(F.regexp_replace("text", "!", "")))
        .withWatermark("event_time", "2 hours")
        .groupBy(F.window("event_time", "1 hour", "10 minutes"), "product_id")
        .agg(
            F.count("*").alias("review_count"),
            F.approx_count_distinct("user_id").alias("unique_users"),
            F.avg("rating").alias("mean_rating"),
            F.stddev_pop("rating").alias("rating_stddev"),
            F.avg("review_length").alias("mean_review_length"),
            F.avg(F.col("verified_purchase").cast("double")).alias("verified_ratio"),
        )
        .withColumn("reviews_per_user", F.col("review_count") / F.greatest(F.col("unique_users"), F.lit(1)))
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--topic", default="reviews.raw.v1")
    parser.add_argument("--output", default="/opt/project/data/features/stream")
    parser.add_argument("--checkpoint", default="/opt/project/data/checkpoints/review-features")
    parser.add_argument(
        "--available-now",
        action="store_true",
        help="Process all currently available Kafka records and then exit",
    )
    args = parser.parse_args()
    spark = SparkSession.builder.appName("amazon-review-stream-features").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = kafka.select(F.from_json(F.col("value").cast("string"), REVIEW_SCHEMA).alias("r")).select("r.*")
    writer = (
        build_features(parsed)
        .writeStream.outputMode("append")
        .format("parquet")
        .option("path", args.output)
        .option("checkpointLocation", args.checkpoint)
    )
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="30 seconds")
    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
