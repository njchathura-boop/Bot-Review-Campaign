from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F, types as T


WINDOW_SCHEMA_VERSION = "campaign.window.v1"
MAX_EVENTS_PER_WINDOW = 1_000

REVIEW_SCHEMA = T.StructType(
    [
        T.StructField("review_id", T.StringType(), False),
        T.StructField("user_id", T.StringType(), False),
        T.StructField("product_id", T.StringType(), False),
        T.StructField("text", T.StringType(), False),
        T.StructField("rating", T.DoubleType(), False),
        T.StructField("timestamp", T.StringType(), False),
        T.StructField("launch_time", T.StringType(), True),
        T.StructField("verified_purchase", T.BooleanType(), True),
        T.StructField("helpful_votes", T.LongType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("source", T.StringType(), True),
        T.StructField("schema_version", T.StringType(), True),
        T.StructField("hours_since_launch", T.DoubleType(), True),
        T.StructField("replay_job_id", T.StringType(), True),
    ]
)


def _event_timestamp(column: str):
    raw = F.col(column)
    numeric_seconds = raw.cast("double") / F.when(
        raw.cast("double") > 10_000_000_000, F.lit(1000.0)
    ).otherwise(F.lit(1.0))
    return F.when(
        raw.rlike(r"^[0-9]+(?:\.[0-9]+)?$"), F.to_timestamp(F.from_unixtime(numeric_seconds))
    ).otherwise(F.to_timestamp(raw))


def build_analysis_windows(frame):
    """Validate reviews and create cross-product semantic/account/product routes."""
    ignored_route_words = (
        "about",
        "after",
        "again",
        "amazon",
        "because",
        "bought",
        "could",
        "product",
        "purchase",
        "quality",
        "really",
        "review",
        "there",
        "these",
        "thing",
        "using",
        "would",
    )
    enriched = (
        frame.withColumn("event_time", _event_timestamp("timestamp"))
        .filter(
            F.col("review_id").isNotNull()
            & F.col("user_id").isNotNull()
            & F.col("product_id").isNotNull()
            & F.col("event_time").isNotNull()
            & (F.length(F.trim("text")) >= 3)
            & F.col("rating").between(1, 5)
        )
        .withColumn("category", F.coalesce(F.col("category"), F.lit("unknown")))
        .withColumn("timestamp", F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
        .withColumn(
            "route_tokens",
            F.slice(
                F.sort_array(
                    F.array_distinct(
                        F.filter(
                            F.split(
                                F.regexp_replace(F.lower("text"), r"[^a-z0-9]+", " "),
                                r"\s+",
                            ),
                            lambda token: (F.length(token) >= 5)
                            & (~token.isin(*ignored_route_words)),
                        )
                    )
                ),
                1,
                6,
            ),
        )
        .withWatermark("event_time", "2 hours")
    )
    event = F.struct(
        "review_id",
        "user_id",
        "product_id",
        "text",
        "rating",
        "timestamp",
        "launch_time",
        F.coalesce(F.col("verified_purchase"), F.lit(False)).alias("verified_purchase"),
        F.coalesce(F.col("helpful_votes"), F.lit(0)).alias("helpful_votes"),
        "category",
        "replay_job_id",
        F.coalesce(F.col("hours_since_launch"), F.lit(24.0 * 365)).alias(
            "hours_since_launch"
        ),
    )
    event_rows = enriched.select("event_time", "product_id", "user_id", "route_tokens", event.alias("event"))
    routed = (
        event_rows.select(
            "event_time",
            F.lit("product").alias("route_type"),
            F.col("product_id").alias("route_key"),
            "event",
        )
        .unionByName(
            event_rows.select(
                "event_time",
                F.lit("account").alias("route_type"),
                F.col("user_id").alias("route_key"),
                "event",
            )
        )
        .unionByName(
            event_rows.withColumn("route_token", F.explode("route_tokens")).select(
                "event_time",
                F.lit("semantic_token").alias("route_type"),
                F.col("route_token").alias("route_key"),
                "event",
            )
        )
    )
    return (
        routed.groupBy(
            F.window("event_time", "1 hour", "10 minutes"), "route_type", "route_key"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.slice(F.sort_array(F.collect_list(event)), 1, MAX_EVENTS_PER_WINDOW).alias(
                "events"
            ),
        )
        .filter(F.col("event_count") >= 3)
        .select(
            F.lit(WINDOW_SCHEMA_VERSION).alias("schema_version"),
            F.concat_ws(
                "|", "route_type", "route_key", F.col("window.start").cast("string")
            ).alias("group_id"),
            "route_type",
            "route_key",
            "event_count",
            F.date_format(F.col("window.start"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
                "window_start"
            ),
            F.date_format(F.col("window.end"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(
                "window_end"
            ),
            "events",
            (F.col("event_count") > MAX_EVENTS_PER_WINDOW).alias("truncated"),
        )
    )


def main():
    parser = argparse.ArgumentParser(
        description="Build cross-product semantic/account/product event-time routes"
    )
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--input-topic", default="reviews.raw.v1")
    parser.add_argument("--output-topic", default="reviews.analysis-windows.v1")
    parser.add_argument("--checkpoint", default="/opt/project/data/checkpoints/analysis-windows-v1")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("cross-product-routing-windows")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = kafka.select(
        F.from_json(F.col("value").cast("string"), REVIEW_SCHEMA).alias("review")
    ).select("review.*")
    encoded = build_analysis_windows(parsed).select(
        F.col("group_id").cast("string").alias("key"),
        F.to_json(F.struct("*")).cast("string").alias("value"),
    )
    # Update mode emits active aggregate windows during a live stream. Append mode
    # would wait until the two-hour watermark closes the window, which can leave a
    # short campaign replay invisible until a much later event advances the watermark.
    writer = (
        encoded.writeStream.outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", args.output_topic)
        .option("checkpointLocation", args.checkpoint)
    )
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(
        processingTime="30 seconds"
    )
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
