import os
import json
from sqlalchemy import create_engine, MetaData, text, Table
import pandas as pd
import urllib.parse

def query_prod(loanIds, userIds):

    # MySQL connection parameters
    user = ''
    password = ''
    host = 'baubap-prod-aurora3-mysql8-cluster.cluster-ro-cdtaivnuur7p.us-east-2.rds.amazonaws.com'
    database = 'baubap'

    # Create MySQL connection string
    connection_string = f'mysql+pymysql://{user}:{password}@{host}/{database}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)

    # Convert list to comma-separated string
    comma_separated_loanIds = ', '.join(loanIds)
    comma_separated_userIds = ', '.join(userIds)

    columns = ['*']
    #columns = ['workId', 'userId', 'loanId', 'workType', 'workActivity', 'workName', 'income', 'monthlyIncome', 'educationLevel', 'incomeChannel', 'workTenure', 'incomeDocType', 'incomeDocTypeOther', 'sector', 'activity', 'incomeFrequency', 'nextIncomeDate', 'incomeDay', 'incomeDayName', 'timestamp']
    columns = ', '.join(columns)

    table_name = 'indicators'

    # Define your SQL query
    #sql_query = f"SELECT * FROM loans WHERE loanId IN ({comma_separated_loanIds});"
    #sql_query = select([table]).where(table.c.loanId.in_(loanIds))
    #sql_query = f"SELECT {columns} FROM {table_name} WHERE userId IN ({comma_separated_userIds});"
    sql_query = f"SELECT {columns} FROM {table_name};"

    # Execute the query
    with engine.connect() as connection:
        result = connection.execute(text(sql_query))

        # Fetch all rows from the result
        rows = result.fetchall()

   
    # Convert the rows to a pandas DataFrame
    df = pd.DataFrame(rows, columns=result.keys())

    # Close the database connection
    engine.dispose()
    
    return df



def write_to_local(df):

    # MySQL connection parameters
    user = ''
    password = ''
    host = 'localhost:3306'
    database = 'baubap'

    # Encode the password
    encoded_password = urllib.parse.quote(password, safe='')

    # Create MySQL connection string
    connection_string = f'mysql+pymysql://{user}:{encoded_password}@{host}/{database}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    

    # replace invalide value for dates in paidAtDate, disbursedDate, dueDate, registrationDate, mostRecentTransactionDate, oldestTransactionDate
    '''
    df['paidAtDate'] = df['paidAtDate'].replace('0000-00-00', None)
    df['disbursedDate'] = df['disbursedDate'].replace('0000-00-00', None)
    df['dueDate'] = df['dueDate'].replace('0000-00-00', None)
    df['registrationDate'] = df['registrationDate'].replace('0000-00-00', None)
    df['mostRecentTransactionDate'] = df['mostRecentTransactionDate'].replace('0000-00-00', None)
    df['oldestTransactionDate'] = df['oldestTransactionDate'].replace('0000-00-00', None)
    '''

    # replace nans
    df = df.map(lambda x: 0 if pd.isna(x) else x)

    # Use pandas to import data from DataFrame to MySQL table
    #df.to_sql(name=table_name, con=engine, if_exists='append', index=False, method='multi')
    #df.to_sql(name=table_name, con=engine, if_exists='append', index=False, method=insert_on_duplicate)

    # Specify the name of the MySQL table to which you want to import the data
    table_name = 'indicators'

    for index, row in df.iterrows():
        # Execute the insert statement

        with engine.begin() as connection:

            columns = []
            values = []
            for column, value in row.items():
                columns.append(column)
                values.append(value)
            
            columns = ', '.join(columns)
            values = [str(item) for item in values]
            #escape colon
            values = [item.replace(':', r'\:') for item in values]
            #escape '
            values = [item.replace('\'', r'\'') for item in values]

            values = ['\'' + item + '\'' for item in values]
            values = ', '.join(values)


            query = f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({values});"

            try:
                connection.execute(text(query))  
            except:
                print(query)
                print(columns)
                print(values)
                raise Exception('Bad insert')

    # Close the database connection
    engine.dispose()


def process_json_data(data):
    # Insert your code here to process the read JSON data
    loanIds = []
    userIds = []
    batchSize = 1000
    loanIdsInBatch = 0
    total = len(data["results"])
    numOfBatches = (total // batchSize) + (total % batchSize != 0)
    batchId = 1
    for entry in data["results"]:
        loanIds.append(str(int(entry["loanId"])))
        userIds.append(str(int(entry["userId"])))
        loanIdsInBatch += 1
        if loanIdsInBatch == batchSize:

            #eliminate duplicate userIds
            unique_userIds = []
            [unique_userIds.append(x) for x in userIds if x not in unique_userIds]    

            print(f'Batch {batchId} of {numOfBatches}')
            df = query_prod(loanIds=loanIds, userIds=unique_userIds)
            write_to_local(df)    
            loanIdsInBatch = 0
            loanIds = []
            userIds = []
            batchId += 1

def read_json_files_in_folder(folder_path):
    filenameId = 1
    for filename in os.listdir(folder_path):
        if filename.endswith('.json'):

            filepath = os.path.join(folder_path, filename)

            with open(filepath, 'r') as file:
                print(f'Loading {filepath} #: {filenameId}')
                json_data = json.load(file)
                # Call function to process JSON data
                process_json_data(json_data)

            filenameId += 1

# Specify the folder containing the JSON files
folder_path = './input/didi_batches'

# Call function to read JSON files in the folder
read_json_files_in_folder(folder_path)
