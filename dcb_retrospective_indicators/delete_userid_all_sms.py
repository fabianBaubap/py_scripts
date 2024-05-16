import os
import json
from sqlalchemy import create_engine, MetaData, text, Table
import pandas as pd
import urllib.parse
import yaml

def read_db_config():
    file_path = 'configurations.yaml'
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['database_test']

def delete_sms_dev(userId):

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
    table_name = 'smsLog'
    
    query = f"DELETE FROM {table_name} WHERE userId = {userId};"

    with engine.begin() as connection:
        try:
            connection.execute(text(query))
        except:
            print(query)
            raise Exception('Bad delete')

    # Close the database connection
    engine.dispose()


userId = 0
delete_sms_dev(2495274)
