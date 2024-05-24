import pandas as pd
import os

# Directory containing the CSV files
csv_directory = './csv'

# Lists to store loanId and newInstalledApps values
loan_ids = []
installed_apps = []

# Iterate over each CSV file in the directory
for filename in os.listdir(csv_directory):
    if filename.endswith('.csv'):
        print(f"Reading filename: {filename}")
        # Read the CSV file
        df = pd.read_csv(os.path.join(csv_directory, filename))
        
        # Append loanId and newInstalledApps values to the lists
        loan_ids.extend(df['loanId'].tolist())
        installed_apps.extend(df['newInstalledApps'].tolist())

print(f"Number of loanIds retrieved: {len(loan_ids)}")

# Create a DataFrame with loanId and newInstalledApps columns
data = {'loanId': loan_ids, 'newInstalledApps': installed_apps}
df_combined = pd.DataFrame(data)

# Write the DataFrame to a single Parquet file
parquet_file = 'excluded_loanIds.parquet'
df_combined.to_parquet(parquet_file, index=False)

print(f"Loan IDs stored in {parquet_file}")
