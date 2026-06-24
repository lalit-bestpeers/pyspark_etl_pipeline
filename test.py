import pandas as pd

parquet_path = r"C:\Users\developer\Desktop\projects\Test_task\output\fact_user_daily\2024-01-15\8846312b6872473d8966b53630a0787e-0.parquet"

df = pd.read_parquet(parquet_path)

print("\nColumns:")
print(df.columns.tolist())

print("\nShape:")
print(df.shape)

print("\nData:")
print(df.to_string(index=False))