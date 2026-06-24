$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"
$env:PYSPARK_PYTHON = "C:\Users\developer\AppData\Local\Programs\Python\Python311\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "C:\Users\developer\AppData\Local\Programs\Python\Python311\python.exe"

python src/etl_job.py --input_path "Data/*.json" --schema_path "Data/schema.json" --output_path "output/fact_user_daily"
