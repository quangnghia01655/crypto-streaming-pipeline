import pandas as pd
import os

folder_path = "data/output/crypto_aggregates"

# Find all valid Parquet files in the output directory
parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]

if parquet_files:
    # Read and print the most recently saved Parquet file
    file_to_read = os.path.join(folder_path, parquet_files[-1])
    df = pd.read_parquet(file_to_read)
    
    print(f"Data Lake Verification Successful! Read file: {parquet_files[-1]}\n")
    print(df.to_string(index=False))
else:
    print("⏳ No Parquet files found. Ensure the Spark stream has run for at least 1 minute.")