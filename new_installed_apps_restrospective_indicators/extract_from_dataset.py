import pyarrow.parquet as pq
import pandas as pd
import yaml
from sqlalchemy import create_engine, text
import urllib.parse

# Path to the Parquet file
parquet_file = 'excluded_loanIds.parquet'

# Load the Parquet file into a DataFrame
print(f"Extracting loanIds to exclude from parquet...")
excluded_loanIds = pd.read_parquet(parquet_file)

# Function to append DataFrame to Parquet file
def append_to_parquet(df, parquet_file):
    df.to_parquet(parquet_file, index=False)

def read_db_config():
    file_path = 'configurations.yaml'
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['database_prod']

def read_from_prod():

    # MySQL connection parameters
    db_config = read_db_config()

    user = db_config['user']
    password = db_config['password']
    host = db_config['host']
    database = db_config['database']

    # Encode the password
    encoded_password = urllib.parse.quote(password, safe='')

    # Create MySQL connection string
    connection_string = f'mysql+pymysql://{user}:{encoded_password}@{host}/{database}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)

    # Specify the name of the MySQL table to which you want to import the data
    table_name = 'datasets_microloans'

    columns = ['*']
    #columns = ['workId', 'userId', 'loanId', 'workType', 'workActivity', 'workName', 'income', 'monthlyIncome', 'educationLevel', 'incomeChannel', 'workTenure', 'incomeDocType', 'incomeDocTypeOther', 'sector', 'activity', 'incomeFrequency', 'nextIncomeDate', 'incomeDay', 'incomeDayName', 'timestamp']
    columns = ', '.join(columns)

    num_of_rows = 6502937
    block_size = 50000

    num_of_blocks = num_of_rows // block_size + ((num_of_rows % block_size) != 0)
    print(f"Num of blocks: {num_of_blocks}")

    remaining_rows = num_of_rows
    offset = 0
    block_id = 1

    while remaining_rows != 0:

        if remaining_rows > block_size:
            start = offset + 1
            end = offset + block_size
            offset += block_size
            remaining_rows -= block_size
        else:
            start = offset + 1
            end = offset + remaining_rows
            offset += remaining_rows
            remaining_rows = 0

        print(f"[block {block_id} of {num_of_blocks}] Retreiving datasetCreditScoreLoanIds from {start} to {end}")
        sql_query = f"SELECT {columns} FROM {table_name} WHERE datasetCreditScoreLoanId >= {start} AND datasetCreditScoreLoanId <= {end} AND loanIdx = 0;"

        # Execute the query
        with engine.connect() as connection:
            result = connection.execute(text(sql_query))

            # Fetch all rows from the result
            rows = result.fetchall()

        # Convert the rows to a pandas DataFrame
        df = pd.DataFrame(rows, columns=result.keys())
        print(f"[block {block_id} of {num_of_blocks}] {len(df)} rows retreived")

        filtered_df = df[~df['loanId'].isin(excluded_loanIds['loanId'])]
        print(f"[block {block_id} of {num_of_blocks}] {len(filtered_df)} rows left from filtering")

        filename = f"./parquets/loanIds_to_calculate_{block_id}.parquet"
        append_to_parquet(df, filename)

        print(f"[block {block_id} of {num_of_blocks}] Results saved in {filename}")

        block_id += 1

    engine.dispose()
    
read_from_prod()