import argparse
import glob
import json as json_mod
import os
# pyrefly: ignore [missing-import]
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark import Row
from pyspark.sql import SparkSession

from schema_loader import load_schema
from transformations import (
    clean_events,
    add_event_date,
    build_fact_user_daily
)
from utils import setup_logger


def create_spark():

    return (
        SparkSession.builder
        .appName("FactUserDailyETL")
        .getOrCreate()
    )


def read_input(input_path):
    if glob.has_magic(input_path):
        files = glob.glob(input_path)
    else:
        files = glob.glob(input_path.rstrip("/\\") + "/*.json")
    records = []
    for fpath in files:
        with open(fpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json_mod.loads(line))
                except json_mod.JSONDecodeError:
                    pass
    return records


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input_path",
        required=True
    )

    parser.add_argument(
        "--schema_path",
        required=True
    )

    parser.add_argument(
        "--output_path",
        required=True
    )

    args = parser.parse_args()

    logger = setup_logger()

    spark = create_spark()

    logger.info("Loading schema")

    schema = load_schema(
        args.schema_path
    )

    logger.info("Reading JSON files")

    raw_records = read_input(args.input_path)

    logger.info(f"Read {len(raw_records)} raw records")

    lines_rdd = spark.sparkContext.parallelize(
        [json_mod.dumps(r) for r in raw_records]
    )
    raw_df = spark.read.schema(schema).json(lines_rdd)

    logger.info(
        f"Input rows: {raw_df.count()}"
    )

    clean_df = clean_events(raw_df)

    enriched_df = add_event_date(clean_df)

    fact_df = build_fact_user_daily(
        enriched_df
    )

    logger.info(
        f"Output rows: {fact_df.count()}"
    )

    pd_df = fact_df.toPandas()
    table = pa.Table.from_pandas(pd_df)
    os.makedirs(args.output_path, exist_ok=True)
    pq.write_to_dataset(
        table,
        root_path=args.output_path,
        partitioning=["date"],
        existing_data_behavior="overwrite_or_ignore",
    )

    logger.info(
        "ETL completed successfully"
    )

    spark.stop()


if __name__ == "__main__":
    main()