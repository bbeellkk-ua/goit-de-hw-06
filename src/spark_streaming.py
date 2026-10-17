"""Aggregate Kafka sensor events with Spark and publish configured alerts."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from config import (
    BOOTSTRAP_SERVERS,
    BUILDING_SENSORS_TOPIC,
    GENERAL_ALERTS_TOPIC,
    UNUSED_LIMIT,
)


SENSOR_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("temperature", DoubleType(), nullable=False),
        StructField("humidity", DoubleType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False),
    ]
)

ALERTS_SCHEMA = StructType(
    [
        StructField("id", StringType(), nullable=False),
        StructField("humidity_min", DoubleType(), nullable=False),
        StructField("humidity_max", DoubleType(), nullable=False),
        StructField("temperature_min", DoubleType(), nullable=False),
        StructField("temperature_max", DoubleType(), nullable=False),
        StructField("code", StringType(), nullable=False),
        StructField("message", StringType(), nullable=False),
    ]
)


def build_alerts(averages: DataFrame, conditions: DataFrame) -> DataFrame:
    """Cross join window averages with static conditions and retain matches.

    A value of -999 means that a particular boundary is not active. At least
    one boundary for a metric must be active for that metric to match. This
    supports the supplied two-sided ranges and remains valid if a future CSV
    uses only a minimum or only a maximum.
    """
    temperature_has_limit = (
        (F.col("temperature_min") != UNUSED_LIMIT)
        | (F.col("temperature_max") != UNUSED_LIMIT)
    )
    temperature_matches = (
        temperature_has_limit
        & (
            (F.col("temperature_min") == UNUSED_LIMIT)
            | (F.col("temperature_avg") >= F.col("temperature_min"))
        )
        & (
            (F.col("temperature_max") == UNUSED_LIMIT)
            | (F.col("temperature_avg") <= F.col("temperature_max"))
        )
    )

    humidity_has_limit = (
        (F.col("humidity_min") != UNUSED_LIMIT)
        | (F.col("humidity_max") != UNUSED_LIMIT)
    )
    humidity_matches = (
        humidity_has_limit
        & (
            (F.col("humidity_min") == UNUSED_LIMIT)
            | (F.col("humidity_avg") >= F.col("humidity_min"))
        )
        & (
            (F.col("humidity_max") == UNUSED_LIMIT)
            | (F.col("humidity_avg") <= F.col("humidity_max"))
        )
    )

    return (
        averages.crossJoin(F.broadcast(conditions))
        .where(temperature_matches | humidity_matches)
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.round("temperature_avg", 2).alias("temperature_avg"),
            F.round("humidity_avg", 2).alias("humidity_avg"),
            F.col("code"),
            F.col("message"),
        )
    )


def main() -> None:
    conditions_path = os.getenv(
        "ALERTS_CONDITIONS_PATH",
        "/workspace/data/alerts_conditions.csv",
    )
    checkpoint_dir = os.getenv("SPARK_CHECKPOINT_DIR", "/checkpoints/alerts")
    trigger_interval = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")

    spark = (
        SparkSession.builder.appName("goit-de-hw-06-spark-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    conditions = (
        spark.read.option("header", True)
        .schema(ALERTS_SCHEMA)
        .csv(conditions_path)
    )
    if not conditions.take(1):
        raise ValueError(f"No alert conditions found in {conditions_path}")

    print(f"Reading sensor events from Kafka topic: {BUILDING_SENSORS_TOPIC}")
    print(f"Publishing alerts to Kafka topic: {GENERAL_ALERTS_TOPIC}")
    print("Alert conditions:")
    conditions.orderBy("id").show(truncate=False)

    kafka_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", BUILDING_SENSORS_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    sensor_events = (
        kafka_events.select(
            F.from_json(F.col("value").cast("string"), SENSOR_SCHEMA).alias(
                "reading"
            ),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            F.col("reading.id").alias("id"),
            F.col("reading.temperature").alias("temperature"),
            F.col("reading.humidity").alias("humidity"),
            F.coalesce(
                F.to_timestamp("reading.timestamp"),
                F.col("kafka_timestamp"),
            ).alias("event_time"),
        )
        .where(
            F.col("id").isNotNull()
            & F.col("temperature").isNotNull()
            & F.col("humidity").isNotNull()
            & F.col("event_time").isNotNull()
        )
    )

    averages = (
        sensor_events.withWatermark("event_time", "10 seconds")
        .groupBy(F.window("event_time", "1 minute", "30 seconds"))
        .agg(
            F.avg("temperature").alias("temperature_avg"),
            F.avg("humidity").alias("humidity_avg"),
        )
    )

    alerts = build_alerts(averages, conditions)
    kafka_output = alerts.select(
        F.col("code").cast("string").alias("key"),
        F.to_json(
            F.struct(
                "window_start",
                "window_end",
                "temperature_avg",
                "humidity_avg",
                "code",
                "message",
            )
        ).alias("value"),
    )

    query = (
        kafka_output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("topic", GENERAL_ALERTS_TOPIC)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start()
    )

    print(
        "Spark query started: watermark=10 seconds, "
        "window=1 minute, slide=30 seconds",
        flush=True,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
