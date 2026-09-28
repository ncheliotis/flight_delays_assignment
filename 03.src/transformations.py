from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import coalesce, col, lit, make_date, row_number

from config import FLIGHTS_BK


# Convert HHMM values like 1435 to minutes from midnight.
# The source uses 2400 for midnight, so we convert it back to 0.
def hhmm_to_minutes(column_name: str):

    value = col(column_name).cast("int")
    return ((value / 100).cast("int") * 60 + (value % 100)) % 1440


# Apply the same transformations to both the historical data and the daily feed.
def prepare_flights_data(df: DataFrame) -> DataFrame:

    df = df.withColumn("FLIGHT_DATE", make_date(col("YEAR"), col("MONTH"), col("DAY")))

    # TAIL_NUMBER is part of the business key, but can be NULL.
    # Replace NULL with UNKNOWN so it can be matched during the merge.
    df = df.withColumn("TAIL_NUMBER", coalesce(col("TAIL_NUMBER"), lit("UNKNOWN")))

    df = (
        df.withColumn(
            "SCHEDULED_DEPARTURE_MINUTE", hhmm_to_minutes("SCHEDULED_DEPARTURE")
        )
        .withColumn("DEPARTURE_MINUTE", hhmm_to_minutes("DEPARTURE_TIME"))
        .withColumn("ARRIVAL_MINUTE", hhmm_to_minutes("ARRIVAL_TIME"))
    )

    return df


# Rank records with the same business key so we can keep only one.
# Newest ingestion wins, followed by operated flights and then origin airport
# to make the result consistent between runs.
def add_dedup_rank(df: DataFrame) -> DataFrame:

    dedup_window = Window.partitionBy(*FLIGHTS_BK).orderBy(
        col("INGESTED_AT").desc(),
        col("CANCELLED").asc(),
        col("ORIGIN_AIRPORT").asc(),
    )

    return df.withColumn("ROW_RANK", row_number().over(dedup_window))


def deduplicate_flights(df: DataFrame) -> DataFrame:

    return add_dedup_rank(df).filter(col("ROW_RANK") == 1).drop("ROW_RANK")


# task 1 solution:
# Let the daily feed replace existing records with the same business key.
# Remove the old records first, then add the new ones.
def upsert_flights(existing_df: DataFrame, feed_df: DataFrame) -> DataFrame:

    feed_df = deduplicate_flights(prepare_flights_data(feed_df))

    existing_without_matches = existing_df.join(feed_df, on=FLIGHTS_BK, how="left_anti")

    return existing_without_matches.unionByName(feed_df)
