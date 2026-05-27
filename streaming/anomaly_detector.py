import os
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, avg, lit,
    current_timestamp, when
)
from pyspark.sql.avro.functions import from_avro

load_dotenv()

KAFKA_BOOTSTRAP     = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SPARK_KAFKA_PACKAGE = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
SPARK_AVRO_PACKAGE  = "org.apache.spark:spark-avro_2.12:3.5.0"

# thresholds — tune these based on your observed baseline
SPIKE_THRESHOLD       = 90   # page views per minute above this = spike (baseline ~84 at 10 events/sec * 70% * 60s / 7 pages)
CONVERSION_THRESHOLD  = 0.30 # purchase:cart ratio below this = drop (baseline ~0.4 at 4%/10%)
ABANDONMENT_THRESHOLD = 4.0  # cart:purchase ratio above this = surge


def get_schema(topic):
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{topic}-value/versions/latest"
    return requests.get(url).json()["schema"]


def build_spark_session():
    packages = f"{SPARK_KAFKA_PACKAGE},{SPARK_AVRO_PACKAGE}"
    return (
        SparkSession.builder
        .appName("AnomalyDetector")
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_topic(spark, topic):
    schema_str = get_schema(topic)
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .select(col("value").substr(6, 100000).alias("avro_value"))
        .select(from_avro(col("avro_value"), schema_str).alias("data"))
        .select("data.*")
        .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))
    )


def detect_traffic_spike(df_pageviews):
    # tumbling 1-min window: count total pageviews
    # flag any window exceeding the static threshold
    # no joins needed — single stream aggregation
    windowed = (
        df_pageviews
        .groupBy(window("event_time", "1 minute"))
        .agg(count("*").alias("view_count"))
    )

    return (
        windowed
        .withColumn("is_spike", col("view_count") > lit(SPIKE_THRESHOLD))
        .withColumn(
            "severity",
            when(col("view_count") > lit(SPIKE_THRESHOLD * 1.5), "HIGH")
            .when(col("is_spike"), "MEDIUM")
            .otherwise("NORMAL")
        )
        .filter(col("is_spike"))
        .select(
            lit("traffic_spike").alias("detector"),
            col("window.start").alias("window_start"),
            col("view_count").cast("double").alias("metric_value"),
            lit(float(SPIKE_THRESHOLD)).alias("threshold"),
            col("severity"),
            current_timestamp().alias("detected_at")
        )
    )


def detect_conversion_drop(df_purchases):
    # use purchase topic alone — low avg order count per window signals drop
    # a full conversion rate needs both streams which requires append mode + watermark
    # this simplified version flags windows with suspiciously low purchase counts
    windowed = (
        df_purchases
        .groupBy(window("event_time", "5 minutes", "1 minute"))
        .agg(
            count("*").alias("purchase_count"),
            avg("total_value").alias("avg_order_value")
        )
    )

    # baseline: ~24 purchases per 5 min at 10 events/sec * 4% * 300s
    # flag when count drops below 40% of baseline (< ~10 purchases per window)
    baseline = 24
    drop_threshold = int(baseline * 0.4)

    return (
        windowed
        .withColumn("is_drop", col("purchase_count") < lit(drop_threshold))
        .filter(col("is_drop"))
        .select(
            lit("conversion_drop").alias("detector"),
            col("window.start").alias("window_start"),
            col("purchase_count").cast("double").alias("metric_value"),
            lit(float(drop_threshold)).alias("threshold"),
            lit("HIGH").alias("severity"),
            current_timestamp().alias("detected_at")
        )
    )


def detect_abandonment_surge(df_cart):
    # cart topic alone — high cart volume with no corresponding purchases
    # flag windows where cart events spike above threshold
    # a true abandonment rate needs both streams — simplified here for single-stream
    windowed = (
        df_cart
        .groupBy(window("event_time", "5 minutes", "1 minute"))
        .agg(count("*").alias("cart_count"))
    )

    # baseline ~30 cart events per 5 min at 10 events/sec * 10% * 300s
    # flag when cart count exceeds 1.5x baseline suggesting a surge
    surge_threshold = 45

    return (
        windowed
        .withColumn("is_surge", col("cart_count") > lit(surge_threshold))
        .filter(col("is_surge"))
        .select(
            lit("abandonment_surge").alias("detector"),
            col("window.start").alias("window_start"),
            col("cart_count").cast("double").alias("metric_value"),
            lit(float(surge_threshold)).alias("threshold"),
            lit("MEDIUM").alias("severity"),
            current_timestamp().alias("detected_at")
        )
    )


def write_anomalies(df, query_name):
    return (
        df.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("numRows", 10)
        .queryName(query_name)
        .trigger(processingTime="15 seconds")
        .start()
    )


def main():
    print("Starting anomaly detector...")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading topics...")
    pv   = read_topic(spark, "clickstream.pageviews")
    cart = read_topic(spark, "clickstream.cart")
    pur  = read_topic(spark, "clickstream.purchases")

    print("Starting detectors...")
    write_anomalies(detect_traffic_spike(pv),        "traffic_spike")
    write_anomalies(detect_conversion_drop(pur),     "conversion_drop")
    write_anomalies(detect_abandonment_surge(cart),  "abandonment_surge")

    print("\nAnomaly detectors running. Ctrl+C to stop.\n")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()