import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    max as spark_max,
    window,
    sum as spark_sum,
    count as spark_count,
    to_json,
    struct,
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Logger setup
logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger("order_sales_batch_agg")

def main():
    try:
        spark = (
            SparkSession.builder.appName("OrderSalesMinuteAggregator")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        logger.info("Spark session started")

        # Kafka Config
        KAFKA_BROKERS = "kafka-broker:9092"  # Placeholder, update as needed
        SOURCE_TOPIC = "order"
        TARGET_TOPIC = "order-sales"
        CONSUMER_GROUP = "order-sales-batch-agg"

        # Schema for order messages
        order_schema = (
            StructType()
            .add("order_id", StringType())
            .add("user_id", StringType())
            .add("amount", FloatType())
            .add("order_time", TimestampType())
        )

        # Read from Kafka topic as streaming DataFrame
        df_raw = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("subscribe", SOURCE_TOPIC)
            .option("startingOffsets", "latest")
            .option("groupId", CONSUMER_GROUP)
            .load()
        )

        logger.info("Reading from Kafka topic: %s", SOURCE_TOPIC)

        # Parse value and extract order JSON
        df_orders = df_raw.withColumn(
            "order_json", from_json(col("value").cast("string"), order_schema)
        )

        df_orders = df_orders.select(
            col("order_json.order_id").alias("order_id"),
            col("order_json.user_id").alias("user_id"),
            col("order_json.amount").alias("amount"),
            col("order_json.order_time").alias("order_time"),
        ).where(col("order_id").isNotNull())

        # Deduplicate orders: Keep latest per order_id
        windowed = df_orders.withWatermark("order_time", "2 minutes")
        deduped = windowed.withColumn(
            "max_time",
            spark_max("order_time").over(
                Window.partitionBy("order_id").orderBy(col("order_time").desc())
            ),
        ).where(col("order_time") == col("max_time")).drop("max_time")

        # Aggregate sales data every single minute
        agg = (
            deduped.withWatermark("order_time", "2 minutes")
            .groupBy(window(col("order_time"), "1 minute").alias("minute_window"))
            .agg(
                spark_sum("amount").alias("total_sales"),
                spark_count("order_id").alias("order_count"),
            )
            .select(
                col("minute_window.start").alias("window_start"),
                col("minute_window.end").alias("window_end"),
                col("total_sales"),
                col("order_count"),
            )
        )

        # Prepare for Kafka sink (serialize as JSON)
        output_df = agg.withColumn(
            "value",
            to_json(
                struct(
                    col("window_start"),
                    col("window_end"),
                    col("total_sales"),
                    col("order_count"),
                )
            ),
        )

        # Write to Kafka topic
        (
            output_df.write.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("topic", TARGET_TOPIC)
            .option("checkpointLocation", "/tmp/checkpoints/order-sales-agg")
            .save()
        )
        logger.info("Triggered write to topic: %s", TARGET_TOPIC)

    except Exception as e:
        logger.error("Error running the batch aggregation: %s", str(e))
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
