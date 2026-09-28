# %%
import os
import sys

from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    count,
    lit,
    make_date,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    avg,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SRC_PATH = PROJECT_ROOT / "03.src"

sys.path.append(str(SRC_PATH))

from config import BRONZE_FLIGHTS_PATH, FLIGHTS_BK

from business_data_quality import check_nulls, is_blank

spark = (
    SparkSession.builder.appName("Flights_Exploration")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Read the data from Bronze to see what we actually have before Silver
flights_df = spark.read.parquet(str(BRONZE_FLIGHTS_PATH))

flights_df = flights_df.withColumn(
    "FLIGHT_DATE", make_date(col("YEAR"), col("MONTH"), col("DAY"))
)

flights_df.cache()

print("Total rows:", flights_df.count())

# %%
# Check how many flights we have per day
rows_per_day = flights_df.groupBy("FLIGHT_DATE").agg(count("*").alias("ROW_COUNT"))

print("Distinct flight days:", rows_per_day.count())

rows_per_day.agg(
    spark_min("FLIGHT_DATE").alias("FIRST_DAY"),
    spark_max("FLIGHT_DATE").alias("LAST_DAY"),
    spark_min("ROW_COUNT").alias("MIN_ROWS"),
    avg("ROW_COUNT").alias("AVG_ROWS"),
    spark_max("ROW_COUNT").alias("MAX_ROWS"),
).show(truncate=False)

# %%
# Check for null values in the columns that make up the business key
# TAIL_NUMBER is the only one with missing values
print(check_nulls(flights_df, FLIGHTS_BK))

# %%
# Check what happens to the flights where TAIL_NUMBER is missing
null_tails = flights_df.filter(is_blank("TAIL_NUMBER"))

print("Blank TAIL_NUMBER:", null_tails.count())

null_tails.groupBy("CANCELLED", "DIVERTED").agg(count("*").alias("ROW_COUNT")).orderBy(
    "CANCELLED", "DIVERTED"
).show(truncate=False)

# Check the same breakdown for all flights so we have something to compare against
flights_df.groupBy("CANCELLED", "DIVERTED").agg(count("*").alias("ROW_COUNT")).orderBy(
    "CANCELLED", "DIVERTED"
).show(truncate=False)

# %%
# Check which types of flights have missing departure/arrival information
# Diverted flights need to be checked separately from cancelled flights
flights_df.groupBy("CANCELLED", "DIVERTED").agg(
    spark_sum(is_blank("DEPARTURE_TIME").cast("int")).alias("NULL_DEPARTURE"),
    spark_sum(is_blank("ARRIVAL_TIME").cast("int")).alias("NULL_ARRIVAL"),
    spark_sum(is_blank("ELAPSED_TIME").cast("int")).alias("NULL_ELAPSED"),
).orderBy("CANCELLED", "DIVERTED").show(truncate=False)

# %%
# Find the business keys that appear more than once
duplicate_keys_df = (
    flights_df.groupBy(FLIGHTS_BK)
    .agg(count("*").alias("ROW_COUNT"))
    .filter(col("ROW_COUNT") > 1)
    .select(FLIGHTS_BK)
    .withColumn("TAIL_NUMBER", coalesce(col("TAIL_NUMBER"), lit("UNKNOWN")))
)

# %%
# Get the full rows for the duplicated business keys so we can see what is different
# Fill missing TAIL_NUMBER values because NULL values would not match in the join
duplicate_records = (
    flights_df.withColumn("TAIL_NUMBER", coalesce(col("TAIL_NUMBER"), lit("UNKNOWN")))
    .join(broadcast(duplicate_keys_df), on=FLIGHTS_BK, how="inner")
    .drop("INGESTED_AT", "SOURCE_FILE")
)

duplicate_records.show(50, truncate=False)

# Compare the duplicated rows to see if they are exact duplicates or conflicting records
# If both counts are the same, the duplicated rows contain different data
print("Duplicate rows:", duplicate_records.count())
print("Distinct among them:", duplicate_records.distinct().count())

# %%
flights_df.unpersist()
