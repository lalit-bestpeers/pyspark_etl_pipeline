#!/bin/bash

spark-submit \
src/etl_job.py \
--input_path "Data/*.json" \
--schema_path Data/schema.json \
--output_path output/fact_user_daily