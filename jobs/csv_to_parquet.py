"""PySpark script to read CSV, filter nulls, and write Parquet
Production-ready with error handling and logging.
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import logging


def setup_logging():
    """Configures logging for the job."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    return logging.getLogger("CsvToParquetJob")

def filter_nulls(df):
    """Filters rows with any null values in the DataFrame.

    Args:
        df (pyspark.sql.DataFrame): Input DataFrame.
    Returns:
        pyspark.sql.DataFrame: DataFrame without rows containing null values.
    """
    return df.na.drop()

def main(input_path: str, output_path: str):
    logger = setup_logging()
    logger.info("Starting CSV to Parquet transformation job")
    spark = SparkSession.builder.appName("CsvToParquetJob").getOrCreate()
    try:
        df = spark.read.option("header", True).csv(input_path)
        logger.info(f"Read data from {input_path} with {df.count()} records.")
        df_clean = filter_nulls(df)
        logger.info(f"Filtered nulls. Records after filtering: {df_clean.count()}.")
        df_clean.write.mode("overwrite").parquet(output_path)
        logger.info(f"Data written to Parquet at {output_path}")
    except AnalysisException as e:
        logger.error(f"Error during processing: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected exception: {e}")
        sys.exit(2)
    finally:
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python csv_to_parquet.py <input_csv_path> <output_parquet_path>")
        sys.exit(99)
    _, input_path, output_path = sys.argv
    main(input_path, output_path)
