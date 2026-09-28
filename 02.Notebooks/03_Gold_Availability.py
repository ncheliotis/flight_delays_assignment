import os
import sys

from pathlib import Path

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    date_add,
    greatest,
    least,
    lit,
    row_number,
    sum as spark_sum,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SRC_PATH = PROJECT_ROOT / "03.src"

sys.path.append(str(SRC_PATH))

from config import SILVER_FLIGHTS_PATH, GOLD_AVAILABILITY_PATH

spark = (
    SparkSession.builder.appName("Gold Availability")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

MINUTES_PER_DAY = 1440

# The night window is 23:00 to 07:00 the next morning.
NIGHT_START = 23 * 60
NIGHT_END = NIGHT_START + 8 * 60
NIGHT_MINUTES = NIGHT_END - NIGHT_START

flights_df = spark.read.parquet(str(SILVER_FLIGHTS_PATH))

print("Silver rows:", flights_df.count())

# Keep flights where we have enough information to calculate their duration.
# We use the scheduled departure plus the delay to keep the correct calendar day.
operated_df = (
    flights_df.filter(
        col("DEPARTURE_DELAY").isNotNull() & col("ELAPSED_TIME").isNotNull()
    )
    .withColumn("START", col("SCHEDULED_DEPARTURE_MINUTE") + col("DEPARTURE_DELAY"))
    .withColumn("END", col("START") + col("ELAPSED_TIME"))
    .select("FLIGHT_DATE", "TAIL_NUMBER", "AIRLINE", "START", "END")
)

operated_df.cache()

print("Flights used:", operated_df.count())

first_date = flights_df.agg({"FLIGHT_DATE": "min"}).first()[0]
last_date = flights_df.agg({"FLIGHT_DATE": "max"}).first()[0]


# Calculate how many minutes of a flight fall inside a given time window.
def overlap(window_start, window_end):

    return greatest(
        least(col("END"), lit(window_end)) - greatest(col("START"), lit(window_start)),
        lit(0),
    )


# Split flights at midnight so each part is counted on the correct day.
previous_day = operated_df.filter(col("START") < 0).select(
    date_add("FLIGHT_DATE", -1).alias("FLIGHT_DATE"),
    "TAIL_NUMBER",
    "AIRLINE",
    overlap(-MINUTES_PER_DAY, 0).alias("MINUTES"),
)

same_day = operated_df.select(
    "FLIGHT_DATE",
    "TAIL_NUMBER",
    "AIRLINE",
    overlap(0, MINUTES_PER_DAY).alias("MINUTES"),
)

next_day = operated_df.filter(col("END") > MINUTES_PER_DAY).select(
    date_add("FLIGHT_DATE", 1).alias("FLIGHT_DATE"),
    "TAIL_NUMBER",
    "AIRLINE",
    overlap(MINUTES_PER_DAY, 2 * MINUTES_PER_DAY).alias("MINUTES"),
)

daily_df = (
    previous_day.unionByName(same_day)
    .unionByName(next_day)
    .filter(col("MINUTES") > 0)
    .filter(col("FLIGHT_DATE").between(lit(first_date), lit(last_date)))
    .groupBy("FLIGHT_DATE", "TAIL_NUMBER", "AIRLINE")
    .agg(spark_sum("MINUTES").alias("FLIGHT_MINUTES"))
)

daily_df.cache()

# The night window crosses midnight, so we check both sides of the flight date.
night_own = operated_df.select(
    "FLIGHT_DATE",
    "TAIL_NUMBER",
    overlap(NIGHT_START, NIGHT_END).alias("MINUTES"),
)

night_previous = operated_df.select(
    date_add("FLIGHT_DATE", -1).alias("FLIGHT_DATE"),
    "TAIL_NUMBER",
    overlap(NIGHT_START - MINUTES_PER_DAY, NIGHT_END - MINUTES_PER_DAY).alias(
        "MINUTES"
    ),
)

night_df = (
    night_own.unionByName(night_previous)
    .filter(col("MINUTES") > 0)
    .groupBy("FLIGHT_DATE", "TAIL_NUMBER")
    .agg(spark_sum("MINUTES").alias("NIGHT_FLIGHT_MINUTES"))
)

# Calculate availability at aircraft level first.
# If the recorded flight time goes over 24 hours, availability stays at zero.
aircraft_day_df = (
    daily_df.groupBy("FLIGHT_DATE", "TAIL_NUMBER")
    .agg(spark_sum("FLIGHT_MINUTES").alias("FLIGHT_MINUTES"))
    .join(night_df, on=["FLIGHT_DATE", "TAIL_NUMBER"], how="left")
    .withColumn("NIGHT_FLIGHT_MINUTES", coalesce(col("NIGHT_FLIGHT_MINUTES"), lit(0)))
    .withColumn(
        "AVAILABILITY",
        lit(MINUTES_PER_DAY) - least(col("FLIGHT_MINUTES"), lit(MINUTES_PER_DAY)),
    )
    .withColumn(
        "NIGHT_AVAILABILITY",
        lit(NIGHT_MINUTES) - least(col("NIGHT_FLIGHT_MINUTES"), lit(NIGHT_MINUTES)),
    )
)

# If an aircraft appears under two airlines on the same day,
# assign its availability to the airline with the most flying time.
airline_window = Window.partitionBy("FLIGHT_DATE", "TAIL_NUMBER").orderBy(
    col("FLIGHT_MINUTES").desc(), col("AIRLINE").asc()
)

main_airline_df = (
    daily_df.withColumn("RANK", row_number().over(airline_window))
    .filter(col("RANK") == 1)
    .select("FLIGHT_DATE", "TAIL_NUMBER", "AIRLINE")
)

gold_df = (
    aircraft_day_df.join(main_airline_df, on=["FLIGHT_DATE", "TAIL_NUMBER"])
    .groupBy("FLIGHT_DATE", "AIRLINE")
    .agg(
        spark_sum("AVAILABILITY").alias("AVAILABILITY"),
        spark_sum("NIGHT_AVAILABILITY").alias("NIGHT_AVAILABILITY"),
    )
    .orderBy("FLIGHT_DATE", "AIRLINE")
)

gold_df.cache()

print("Gold rows:", gold_df.count())

# gold_df.show(20, truncate=False)

gold_df.write.mode("overwrite").partitionBy("FLIGHT_DATE").parquet(
    str(GOLD_AVAILABILITY_PATH)
)

# for assignment purposes to have output in csv
# gold_df.toPandas().to_csv(
#    str(PROJECT_ROOT / "01.data" / "04.Final_Output" / "flights_availability.csv"),
#    index=False,
# )

print("\nGold Availability Completed Successfully.")

operated_df.unpersist()

daily_df.unpersist()

gold_df.unpersist()

spark.stop()
