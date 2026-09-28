from pyspark.sql import Column, DataFrame

from pyspark.sql.functions import col, count, trim, sum as spark_sum

# Validate that the DataFrame contains all required columns.


def validate_required_columns(df: DataFrame, required_columns: list[str]) -> None:

    missing_columns = [
        column for column in required_columns if column not in df.columns
    ]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


# A value is treated as missing if it is NULL, an empty string or whitespace.


def is_blank(column_name: str) -> Column:

    column = col(column_name)
    return column.isNull() | (trim(column.cast("string")) == "")


# Check for missing values in the specified columns of the DataFrame.


def check_nulls(df: DataFrame, columns: list[str]) -> dict[str, int]:

    counts_row = df.select(
        [
            spark_sum(is_blank(column_name).cast("int")).alias(column_name)
            for column_name in columns
        ]
    ).first()

    return {column_name: counts_row[column_name] for column_name in columns}


# Check for duplicate records in the DataFrame based on the specified business key.


def check_duplicates(df: DataFrame, business_key: list[str]) -> int:

    duplicate_count = (
        df.groupBy(business_key)
        .agg(count("*").alias("count"))
        .filter(col("count") > 1)
        .count()
    )
    return duplicate_count
