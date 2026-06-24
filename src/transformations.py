# pyrefly: ignore [missing-import]
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    min,
    max,
    sum,
    to_timestamp,
    to_date,
    coalesce,
    lit
)


def clean_events(df):

    df = (
        df
        .dropDuplicates(["event_id"])
        .filter(col("user_id").isNotNull())
    )

    return df


def add_event_date(df):

    return (
        df
        .withColumn(
            "event_ts",
            to_timestamp("timestamp")
        )
        .withColumn(
            "date",
            to_date("event_ts")
        )
    )


def build_fact_user_daily(df):

    return (
        df.groupBy(
            "user_id",
            "date"
        )
        .agg(
            count("*").alias("event_count"),

            countDistinct(
                "event_type"
            ).alias(
                "distinct_event_types"
            ),

            min(
                "event_ts"
            ).alias(
                "first_event_ts"
            ),

            max(
                "event_ts"
            ).alias(
                "last_event_ts"
            ),

            coalesce(
                sum("revenue"),
                lit(0.0)
            ).alias(
                "total_revenue"
            )
        )
    )