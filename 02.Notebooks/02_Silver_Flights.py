import os
import sys

from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SRC_PATH = PROJECT_ROOT / "03.src"

sys.path.append(str(SRC_PATH))

from config import (
    BRONZE_FLIGHTS_PATH,
    SILVER_FLIGHTS_PATH,
    SILVER_REJECTED_PATH,
    FLIGHTS_BK,
)

from business_data_quality import check_duplicates, check_nulls

from transformations import add_dedup_rank, prepare_flights_data

spark = (
    SparkSession.builder.appName("Silver Flights")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

flights_df = spark.read.parquet(str(BRONZE_FLIGHTS_PATH))

print("Bronze rows:", flights_df.count())

# Prepare the historical Bronze data and rank duplicate business keys.
# The daily feed will use the same preparation during the upsert.
ranked_df = add_dedup_rank(prepare_flights_data(flights_df))

ranked_df.cache()

silver_df = ranked_df.filter(col("ROW_RANK") == 1).drop("ROW_RANK")

# Keep the rows removed during deduplication so we can see why they were rejected.
rejected_df = ranked_df.filter(col("ROW_RANK") > 1).withColumn(
    "REJECT_REASON", lit("DUPLICATE_BUSINESS_KEY")
)

rejected_count = rejected_df.count()

print("Silver rows:", silver_df.count())
print("Rejected rows:", rejected_count)

# Make sure the business key is complete before writing the Silver data.
# If something is wrong, stop here instead of carrying the issue forward.
null_counts = check_nulls(silver_df, FLIGHTS_BK)

if any(null_counts.values()):
    raise ValueError(f"Business key still has missing values: {null_counts}")

remaining_duplicates = check_duplicates(silver_df, FLIGHTS_BK)

if remaining_duplicates > 0:
    raise ValueError(f"Business key is still not unique: {remaining_duplicates} keys")

print("Business key is complete and unique.")

# Store the Silver data partitioned by flight date.
# This will make it easier to work with the daily feed later.
silver_df.write.mode("overwrite").partitionBy("FLIGHT_DATE").parquet(
    str(SILVER_FLIGHTS_PATH)
)

if rejected_count > 0:
    rejected_df.write.mode("overwrite").parquet(str(SILVER_REJECTED_PATH))

print("\nSilver Flights Completed Successfully.")

ranked_df.unpersist()

spark.stop()
