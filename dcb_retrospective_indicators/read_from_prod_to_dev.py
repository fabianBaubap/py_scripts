import os
import json
from sqlalchemy import create_engine, MetaData, text, Table
import pandas as pd
import urllib.parse

def read_from_prod():

    # MySQL connection parameters
    user = ''
    password = ''
    host = 'baubap-prod-aurora3-mysql8-cluster.cluster-ro-cdtaivnuur7p.us-east-2.rds.amazonaws.com'
    database = 'baubap'

    # Create MySQL connection string
    connection_string = f'mysql+pymysql://{user}:{password}@{host}/{database}'

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)

    columns = ['*']
    #columns = ['workId', 'userId', 'loanId', 'workType', 'workActivity', 'workName', 'income', 'monthlyIncome', 'educationLevel', 'incomeChannel', 'workTenure', 'incomeDocType', 'incomeDocTypeOther', 'sector', 'activity', 'incomeFrequency', 'nextIncomeDate', 'incomeDay', 'incomeDayName', 'timestamp']
    columns = ', '.join(columns)

    table_name = 'smsLog'

    ids = [6437825407,6437825403,6437825390,6437825790,6437825789,6437825788,6437825785,6437825784,6437825783,6437825780,6437825779,6437825778,6437825775,6437825774,6437825772,6437825767,6437825760,6437825745,6437825740,6437825721,6437825708,6437825688,6437825681,6437825667,6437825666,6437825665,6437825664,6437825663,6437825662,6437825660,6437825657,6437825655,6437825597,6437825577,6437825550,6437825549,6437825548,6437825547,6437825538,6437825537,6437825536,6437825535,6437825530,6437825529,6437825528,6437825526,6437825521,6437825478,6437825466,6437825457,6437825455,6437825449,6437826005,6437825995,6437825992,6437825991,6437825969,6437825968,6437825967,6437825966,6437825965,6437825964,6437825963,6437825962,6437825958,6437825956,6437825955,6437825954,6437825953,6437825952,6437825950,6437825949,6437825948,6437825947,6437825946,6437825944,6437825941,6437825934,6437825930,6437825918,6437825916,6437825912,6437825911,6437825909,6437825907,6437825888,6437825887,6437825886,6437825885,6437825884,6437825878,6437825877,6437825876,6437825875,6437825874,6437825873,6437825872,6437825871,6437825867,6437825866,6437825865,6437825864,6437825863,6437825862,6437825861,6437825859,6437825858,6437825855,6437825854,6437825850,6437825848,6437825846,6437825843,6437825840,6437825839,6437825829,6437825824,6437825820,6437825815,6437825814,6789683774,6789683762,6789683761,6789683754,6789683753,6789683752,6789683747,6789683744,6789683741,6789683738,6789683734,6789683732,6789683727,6789683724,6789683722,6789683713,6789683712,6789683711,6789683710,6789683706,6789683705,6789683704,6789683703,6789683701,6789683700,6789683698,6789683690,6789683687,6789683684,6789683683,6789683682,6789683674,6789683666,6789683661,6789683658,6789683651,6789683648,6789683642,6789683641,6789683633,6789683631]
    comma_separated_ids = ', '.join(map(str, ids))
    # Define your SQL query
    #sql_query = f"SELECT * FROM loans WHERE loanId IN ({comma_separated_loanIds});"
    #sql_query = select([table]).where(table.c.loanId.in_(loanIds))
    #sql_query = f"SELECT {columns} FROM {table_name} WHERE userId IN ({comma_separated_userIds});"
    sql_query = f"SELECT {columns} FROM {table_name} where smsLogId IN ({comma_separated_ids});"

    # Execute the query
    with engine.connect() as connection:
        result = connection.execute(text(sql_query))

        # Fetch all rows from the result
        rows = result.fetchall()

   
    # Convert the rows to a pandas DataFrame
    df = pd.DataFrame(rows, columns=result.keys())
    df['userId'] = 2494674
    df['loanId'] = 12932454
    print(df)
    # Close the database connection
    engine.dispose()
    
    return df



def write_to_dev(df):

    # MySQL connection parameters
    user = ''
    password = ''
    host = 'baubap-test-aurora-3-mysql-8-cluster.cluster-cdtaivnuur7p.us-east-2.rds.amazonaws.com'
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
    #df = df.map(lambda x: 0 if pd.isna(x) else x)

    # Use pandas to import data from DataFrame to MySQL table
    #df.to_sql(name=table_name, con=engine, if_exists='append', index=False, method='multi')
    #df.to_sql(name=table_name, con=engine, if_exists='append', index=False, method=insert_on_duplicate)

    # Specify the name of the MySQL table to which you want to import the data
    table_name = 'smsLog'

    for index, row in df.iterrows():
        # Execute the insert statement

        with engine.begin() as connection:

            columns = []
            values = []
            for column, value in row.items():
                if column != 'smsLogId':
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


            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"

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
#read_json_files_in_folder(folder_path)

df = read_from_prod()
write_to_dev(df)
