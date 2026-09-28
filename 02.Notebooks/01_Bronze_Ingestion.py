import os
import sys

from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

PROJECT_ROOT = Path(__file__).parent.parent

SRC_PATH = PROJECT_ROOT / "03.src"

sys.path.append(str(SRC_PATH))

from config import (
    FLIGHTS_SOURCE,
    AIRLINES_SOURCE,
    AIRPORTS_SOURCE,
    BRONZE_FLIGHTS_PATH,
    BRONZE_AIRLINES_PATH,
    BRONZE_AIRPORTS_PATH,
    FLIGHTS_BK,
)

from business_data_quality import validate_required_columns

# Raise error if any of the source files do not exist
for source_path in [FLIGHTS_SOURCE, AIRLINES_SOURCE, AIRPORTS_SOURCE]:
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")


spark = (
    SparkSession.builder.appName("Bronze Ingestion").master("local[*]").getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

flights_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(str(FLIGHTS_SOURCE))
    .withColumn("INGESTED_AT", current_timestamp())
    .withColumn("SOURCE", lit(str(FLIGHTS_SOURCE)))
)

airlines_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(str(AIRLINES_SOURCE))
    .withColumn("INGESTED_AT", current_timestamp())
    .withColumn("SOURCE", lit(str(AIRLINES_SOURCE)))
)

airports_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(str(AIRPORTS_SOURCE))
    .withColumn("INGESTED_AT", current_timestamp())
    .withColumn("SOURCE", lit(str(AIRPORTS_SOURCE)))
)

# Validate that the DataFrame contains all required columns.
validate_required_columns(flights_df, FLIGHTS_BK)


# Raw Data as parquet files in the Bronze layer
flights_df.write.mode("overwrite").parquet(str(BRONZE_FLIGHTS_PATH))
airlines_df.write.mode("overwrite").parquet(str(BRONZE_AIRLINES_PATH))
airports_df.write.mode("overwrite").parquet(str(BRONZE_AIRPORTS_PATH))


# view the schema of the flights DataFrame
print("\nFlights DataFrame Schema:")
flights_df.printSchema()

print("\nRows Count:")
print(f"Flights DataFrame Count: {flights_df.count()}")
print(f"Airlines DataFrame Count: {airlines_df.count()}")
print(f"Airports DataFrame Count: {airports_df.count()}")


print("\nBronze Ingestion Completed Successfully.")

spark.stop
