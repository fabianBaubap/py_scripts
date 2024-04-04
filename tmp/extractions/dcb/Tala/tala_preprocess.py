import os
import pandas as pd
import pyarrow.parquet as pq
import re

phrases = [
    'Aunque tu fecha fue ayer, puedes liquidar ahora sin penalizaciones',
    'Tu codigo de verificacion para completar tu registro es',
    'Esta es la ultima llamada. Tu cargo por demora de',
    'Tu codigo de verificacion para resetear tu NIP es',
    'Tu codigo de verificacion para actualizar tus datos bancarios es',
    'Aplicamos un cargo por demora',
    'Tu prestamo fue pagado por completo',
    'es tu codigo Tala',
    'Tu fecha de pago fue ayer. Liquida ahora',
    'Su codigo de verificacion para Tala',
    'Tu código de confirmación es',
    'Tu codigo de verificacion para ingresar a Tala es',
    'Tu pago se esperaba el',
    'tu fecha de vencimiento es hoy',
    'no arriesgues tu historial crediticio',
    'Refiere Tala y gana',
    'Tu préstamo fue aprobado',
    'Verificamos tu identidad',
    'no lastimes tu futuro financiero',
    'El préstamo que te mereces',
    'Tu préstamo aprobado esta por expirar',
    'Tu préstamo te esta esperando',
    'Paga a tiempo para seguir recibiendo crédito'


]

def list_parquets():
    # Get the current directory
    current_dir = os.getcwd()

    # List all files in the directory
    files_in_dir = os.listdir(current_dir)

    # Filter out only files ending with ".parquet"
    parquet_files = [file for file in files_in_dir if file.endswith('.parquet')]

    total = 0

    # Read each Parquet file
    for parquet_file in parquet_files:
        file_path = os.path.join(current_dir, parquet_file)
        table = pq.read_table(file_path)
        # Convert the PyArrow Table to a pandas DataFrame
        df = table.to_pandas()
        filtered_df = df
        #filter by phrases
        for phrase in phrases:
            str_pattern = f"{phrase}"
            pattern = re.escape(str_pattern)
            filtered_df = filtered_df[~filtered_df['body'].str.contains(pattern, case = False)]
        
        total += len(filtered_df)
        print(f"Contents of {parquet_file}:")
        pd.set_option('display.max_colwidth', None)
        print(filtered_df)

    print(f"Total of rows: {total}")

def get_body(parquet_file, smsLogId):
    # Get the current directory
    current_dir = os.getcwd()
    file_path = os.path.join(current_dir, parquet_file)
    table = pq.read_table(file_path)
    df = table.to_pandas()
    body = str(df[df['smsLogId'] == smsLogId]['body'].iloc[0])
    print(body)

list_parquets()
#get_body('tala_febrero_2024_2_175000_data.parquet',7010162925)