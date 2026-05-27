import os
import requests
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg
from pyspark.sql.avro.functions import from_avro

load_dotenv()

KAFKA_BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
SCHEMA_REGISTRY_URL  = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SPARK_KAFKA_PACKAGE  = os.getenv("SPARK_KAFKA_PACKAGE", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
SPARK_AVRO_PACKAGE   = "org.apache.spark:spark-avro_2.12:3.5.0"


def get_schema_from_registry(topic):
    # fetch the latest schema string for this topic's value subject
    subject = f"{topic}-value"
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()["schema"]  # returns schema as a JSON string


def build_spark_session():
    packages = f"{SPARK_KAFKA_PACKAGE},{SPARK_AVRO_PACKAGE}"
    return (
        SparkSession.builder
        .appName("ClickstreamConsumer")
        .config("spark.jars.packages", packages)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_topic(spark, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        # Confluent Avro wire format: first 5 bytes = magic byte + schema ID
        # slice them off so from_avro gets clean Avro bytes
        .select(col("value").substr(6, 100000).alias("avro_value"))
    )


def parse_topic(spark, topic):
    schema_str = get_schema_from_registry(topic)
    df = read_kafka_topic(spark, topic)
    # from_avro decodes binary Avro bytes using the schema string
    parsed = df.select(
        from_avro(col("avro_value"), schema_str).alias("data")
    ).select("data.*")
    # convert epoch ms → timestamp for windowing
    return parsed.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))


def agg_pageviews_per_minute(df):
    return (
        df.groupBy(window("event_time", "1 minute"), col("page"))
        .agg(count("*").alias("view_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("page"),
            col("view_count")
        )
    )


def agg_cart_rate(df):
    return (
        df.groupBy(window("event_time", "2 minutes", "30 seconds"))
        .agg(count("*").alias("cart_events"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("cart_events")
        )
    )


def agg_purchase_conversion(df):
    return (
        df.groupBy(window("event_time", "5 minutes", "1 minute"))
        .agg(
            count("*").alias("purchase_count"),
            avg("total_value").alias("avg_order_value")
        )
        .select(
            col("window.start").alias("window_start"),
            col("purchase_count"),
            col("avg_order_value")
        )
    )


def write_stream(df, query_name):
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
    print("Fetching schemas from Schema Registry...")
    # validate registry is reachable before starting Spark
    for topic in ["clickstream.pageviews", "clickstream.cart", "clickstream.purchases"]:
        get_schema_from_registry(topic)
        print(f"  {topic}: OK")

    print("Starting Spark session...")
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Parsing topics with Avro schemas...")
    pv   = parse_topic(spark, "clickstream.pageviews")
    cart = parse_topic(spark, "clickstream.cart")
    pur  = parse_topic(spark, "clickstream.purchases")

    print("Starting streaming queries...")
    write_stream(agg_pageviews_per_minute(pv),   "pageviews_per_minute")
    write_stream(agg_cart_rate(cart),             "cart_rate")
    write_stream(agg_purchase_conversion(pur),    "purchase_conversion")

    print("\nStreaming. Results every 15 seconds. Ctrl+C to stop.\n")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()