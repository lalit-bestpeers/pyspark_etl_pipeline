# Fact User Daily ETL

PySpark ETL job that reads JSON event data, transforms it, and writes a partitioned fact table (`fact_user_daily`) in Parquet format.

## Requirements

| Dependency | Version | Notes |
|---|---|---|
| Python | >= 3.8 | Tested on 3.11 |
| Java | >= 8 / 11 / 17 | JVM required by Spark; tested on Adoptium JDK 17 |
| pip | — | For installing Python packages |

### Python packages

```
pyspark>=3.5,<4.0
pyarrow>=10.0
```

Install with:

```
pip install -r requirements.txt
```

## Project structure

```
Test_task/
├── Data/
│   ├── schema.json              # Spark-compatible schema definition
│   ├── events_2024-01-15_09.json
│   ├── events_2024-01-15_10.json
│   └── events_2024-01-15_23.json
├── src/
│   ├── __init__.py
│   ├── etl_job.py               # Entry point
│   ├── schema_loader.py         # Reads schema.json
│   ├── transformations.py       # clean_events, add_event_date, build_fact_user_daily
│   └── utils.py                 # Logger setup
├── run.sh                       # Linux / macOS launcher
├── run.ps1                      # Windows launcher
├── requirements.txt
└── README.md
```

## Running locally

### Windows

Ensure `JAVA_HOME` points to a JDK installation, then run:

```
.\run.ps1
```

The script sets `JAVA_HOME` and `PYSPARK_PYTHON` automatically. Edit the paths in `run.ps1` if your JDK or Python installation is at a different location.

### Linux / macOS

```
./run.sh
```

If `spark-submit` is not on your PATH, install pyspark with `pip install pyspark` and use `python src/etl_job.py` directly (see below).

### Without the launcher scripts

```bash
# Windows (PowerShell)
$env:JAVA_HOME = "C:\Path\To\Java"
$env:PYSPARK_PYTHON = "C:\Path\To\python.exe"
python src/etl_job.py --input_path "Data/*.json" --schema_path "Data/schema.json" --output_path "output/fact_user_daily"
```

```bash
# Linux / macOS
export JAVA_HOME=/path/to/java
python src/etl_job.py --input_path "Data/*.json" --schema_path "Data/schema.json" --output_path "output/fact_user_daily"
```

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--input_path` | Yes | Glob pattern or directory path to JSON event files |
| `--schema_path` | Yes | Path to the JSON schema definition file |
| `--output_path` | Yes | Output directory for the partitioned Parquet files |

## Data format

### Input

Newline-delimited JSON files. Each line is a JSON object conforming to the schema in `Data/schema.json`.

```json
{"user_id": "u001", "event_type": "page_view", "event_ts": "2024-01-15T14:30:00", "properties": {"page": "/home"}, "revenue": 0.0}
```

### Output

Partitioned Parquet files written by [PyArrow](https://arrow.apache.org/docs/python/parquet.html):

```
output/
└── fact_user_daily/
    ├── date=2024-01-15/
    │   └── *.parquet
    └── date=2024-01-16/
        └── *.parquet
```

Schema (7 columns):

| Column | Type | Description |
|---|---|---|
| `user_id` | string | User identifier |
| `event_count` | long | Total events per user per day |
| `distinct_event_types` | long | Number of unique event types |
| `first_event_ts` | timestamp | Earliest event timestamp for the day |
| `last_event_ts` | timestamp | Latest event timestamp for the day |
| `total_revenue` | double | Sum of revenue for the day |
| `date` | string (partition) | Partition key (`YYYY-MM-DD`) |

## Running in production

### Cluster deployment (Spark on YARN / Kubernetes / Standalone)

The job is designed for local execution. For a production Spark cluster, make the following adjustments:

1. **Replace the input reader** — The current implementation reads files with Python's `json.loads` and parallelizes via RDD. For production-scale data (thousands of files, GBs), switch back to Spark's native reader:
   ```python
   raw_df = spark.read.schema(schema).json(input_path)
   ```
   This requires the Hadoop native libraries to be available on the cluster (pre-configured on most Spark clusters). The custom reader was introduced only to work around missing Hadoop native bindings on Windows.

2. **Replace the output writer** — Use Spark's native `.parquet()` writer for distributed write performance:
   ```python
   fact_df.write.mode("overwrite").partitionBy("date").parquet(output_path)
   ```
   The PyArrow-based writer works for local/small-scale jobs but does not leverage Spark's distributed file system I/O or its output committer (which provides exactly-once semantics on S3, HDFS, etc.).

3. **Speculative execution & retries** — Consider tuning:
   - `spark.sql.sources.commitProtocolClass`
   - `spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2`
   - `spark.speculation=true` / `spark.speculation.interval`

4. **Resource configuration** — Override via `spark-submit`:
   ```bash
   spark-submit \
     --master yarn \
     --deploy-mode cluster \
     --num-executors 10 \
     --executor-memory 8g \
     --driver-memory 4g \
     src/etl_job.py \
     --input_path "s3a://bucket/events/*.json" \
     --schema_path "s3a://bucket/schema.json" \
     --output_path "s3a://bucket/output/fact_user_daily"
   ```

### Job scheduling

Wrap the run command in your scheduler of choice:

- **Apache Airflow**: `BashOperator` or `SparkSubmitOperator`
- **AWS EMR / Step Functions**: Submit as a step
- **cron / Windows Task Scheduler**: Direct shell invocation

### Monitoring

- Spark Web UI (port 4040 by default) for stage-level metrics
- Application logs (`spark.eventLog.enabled=true`)
- The logger from `utils.py` writes structured logs at INFO level; change `WARN` in `spark.sparkContext.setLogLevel("WARN")` for production tuning.

## Known caveats

- The custom JSON reader loads all records into driver memory. For large datasets, use the native `spark.read.json()` on a proper Spark cluster.
- The PyArrow writer runs on the driver — write throughput is limited to a single process. Use Spark's native `.parquet()` writer for distributed output on clusters.
- The shutdown hook may log a `NoSuchFileException` error when cleaning Spark temp directories on Windows — this is harmless and does not affect output.
