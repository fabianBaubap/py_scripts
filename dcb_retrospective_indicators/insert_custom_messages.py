import os
import json
from sqlalchemy import create_engine, MetaData, text, Table
import pandas as pd
import urllib.parse
import yaml
from datetime import datetime


def read_db_config():
    file_path = 'configurations.yaml'
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config['database_test']

def write_to_dev(df):

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
    table_name = 'smsLog'
    reindex = 1
    total = len(df)
    currentId = 1
    for index, row in df.iterrows():
        # Execute the insert statement

        with engine.begin() as connection:

            columns = []
            values = []
            for column, value in row.items():

                if column == 'smsLogId':
                    continue

                if column == '_id':
                    value = reindex
                    reindex += 1

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

            print(f"Query {currentId} of {total}: {query}")
            currentId += 1

            try:
                #print(query)
                connection.execute(text(query))  
            except:
                print(query)
                print(columns)
                print(values)
                raise Exception('Bad insert')

    # Close the database connection
    engine.dispose()

# Get the current datetime object
timestamp = datetime.now().timestamp()

data = [
    {
        'body': 'mensaje de prueba',
    },
    {
        'body': 'Branch Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem DIDI CASH PAGA SALDO PENDIENTE Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.Lorem.'
    },
    {
        'body': 'Branch reporta a Buro de Credito toda falta de pago por lo que no podras obtener ningun credito con ninguna compania. Paga $ 3,526 HOY para evitarlo. وضع ابن الهيثم تصور واضح للعلاقة بين النموذج الرياضي المثالي ومنظومة الظواهر الملحوظة'
    },
    {
        'body': 'RESUELVA SU DEUDA CON BENEFICIOS QUE BRANCH OTORGA LIQUIDE PRESTAMO HOY ACTIVE CONVENIO LLAME A LG+1  AL 5547746157 WHATS 5516112328 ®'
    },
    {
        'body': 'Maria, tu prestamo Branch esta atrasado y debes $ 3,526. Realiza tu pago por $ 3,526 hoy y evita ser reportado al Buro de Credito. ™'
    },
    {
        'body': 'Tu prestamo de Branch sera marcado como mora. ¶ Aun tienes 5 dias para hacer tu pago de $ 3,526 para evitar que te reportemos al Buro de Credito. '
    },
    {
        'body': 'Branch reporta a Buro de Credito toda conאבגדאבגדאבגד falta de pago por lo que no podras obtener ningun credito con ninguna compania. Paga $ 3,526 HOY para evitarlo.'
    },
    {
        'body': 'Branch reporta a Buro de Credito toda falta de pago por lo que no podras obtener ningun credito con ninguna compania. Paga $ 3,526 HOY para evitarlo. وضع ابن الهيثم تصور واضح للعلاقة بين النموذج الرياضي المثالي ومنظومة الظواهر الملحوظة'
    },
    {
        'body': 'Branch reporta a Buro de Credito toda الرياضي المثالي ومنظومة الظواهر الملحو falta de pago por lo que no podras obtener ningun credito con ninguna compania. Paga $ 3,526 HOY para evitarlo.'
    },
    {
        'body': 'didi prestamos'
    },
    {
        'body': ' tala                       '
    },
    {
        'body': 'lendon                  null'
    },
    {
        'body': '''
                Contains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        Newlines Contains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        NewlinesContains        Tabs
        And        Newlines branch
        '''
    },
    {
        'body': 'didi prestamos SALDO PENDIENTE Iñtërnâtiônàlizætiøn☃💪 Iñtërnâtiônàlizætiøn☃💪'
    },
    {
        'body': 'didi prestamos DEUDa pendiente en tala 1E-16'
    },
    {
        'body': 'didi prestamos DEUDa pendiente en tala 0.0001'
    },
    {
        'body': 'didi prestamos DEUDa pendiente en tala null'
    },
    {
        'body': 'lendon iIİı pendiente'
    },
    {
        'body': 'hola jesus, tu prestamo didi prestamos recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo esta por vencer. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo nano recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo banamex recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo kueski cash recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo sanborns recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo palacio recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo c&a cash recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo coppel cash recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo liverpool cash recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    },
    {
        'body': 'hola jesus, tu prestamo tala recien vencio. evita cargos moratorios. escribenos en https://cutt.ly/13dddrr'
    }

]

df = pd.DataFrame(data)
df['loanId'] = 12932990
df['userId'] = 2495274
df['_id'] = 1
df['box'] = 'inbox'
df['timestamp'] = int(timestamp)
df['date'] = int(timestamp)
df['date_sent'] = int(timestamp)
df['group_id'] = 0
df['creator'] = ''
df['service_center'] = ''
df['seen'] = 0
df['thread_id'] = 0
df['address'] = ''

write_to_dev(df)
