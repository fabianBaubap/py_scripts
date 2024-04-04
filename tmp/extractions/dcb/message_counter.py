import os
import pandas as pd
import pyarrow.parquet as pq
import re

# Get the current directory
current_dir = os.getcwd()

# List all files in the directory
files_in_dir = os.listdir(current_dir)

# Filter out only files ending with ".parquet"
parquet_files = [file for file in files_in_dir if file.endswith('.parquet')]

# Read each Parquet file
for parquet_file in parquet_files:
    file_path = os.path.join(current_dir, parquet_file)
    table = pq.read_table(file_path)
    # Convert the PyArrow Table to a pandas DataFrame
    df = table.to_pandas()

    #Tala
    parquet_file_path = f"./Tala/tala_{parquet_file}"
    print(parquet_file_path)
    pattern = r'(?<![a-zA-Z])tala(?![a-zA-Z])'
    filtered_df = df[df['body'].str.contains(pattern, case = False)]
    pattern_unotv = r'unotv'
    filtered_df = filtered_df[~filtered_df['body'].str.contains(pattern_unotv, case=False)]


    filtered_df.to_parquet(parquet_file_path)


    print(f"Contents of {file_path}:")
    print(filtered_df)
