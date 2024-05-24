import os
import pyarrow.parquet as pq
import pyarrow
# Directory containing parquet files
folder_path = './parquets'

# List all parquet files in the folder
parquet_files = [file for file in os.listdir(folder_path) if file.endswith('.parquet')]

# Initialize an empty list to store parquet file data
parquet_data = []

# Read each parquet file and append its data to parquet_data list
for file_name in parquet_files:
    file_path = os.path.join(folder_path, file_name)
    table = pq.read_table(file_path)
    print(table)
    parquet_data.append(table)

# Concatenate all parquet data into a single table
combined_table = pyarrow.concat_tables(parquet_data)

# Write the combined table to a single parquet file
output_file = 'all_loanIds_to_calculate.parquet'
pq.write_table(combined_table, output_file)

print("Combined parquet file saved successfully.")
