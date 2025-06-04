import os
import pandas as pd
import mysql.connector
from mysql.connector import Error
import xlrd

# Conexión a la base de datos MySQL
def create_connection():
    connection = None
    try:
        connection =  mysql.connector.connect(
            host='localhost',
            port='3306',
            user='userDB',
            password='Pasword',
            database='sample'
        )
        print("Conexión exitosa a MySQL")
    except Error as e:
        print(f"Error: '{e}'")
        
    return connection





def insert_data(connection, df):
    cursor = connection.cursor()
    query = """
        INSERT INTO docusign.member_application_form 
        (created_on, envelope_id, status, completed_on, company_legal_name, country_of_incorporation, business_industry, company_website, company_phone_number, state_of_operations, expected_transactions_per_month, sending_volume_per_month, expected_destination_countries, main_purpose_for_transactions, full_name, email, responsible_for_application_process_yes, responsible_for_application_process_no, transaction_type_b2p, transaction_type_b2b, transaction_type_p2p, transaction_type_p2b, num_transactions_per_month, destination_mexico, destination_canada, destination_guatemala, destination_honduras, destination_colombia, destination_ecuador, destination_brazil, destination_argentina, destination_uk, destination_portugal, destination_spain, destination_south_africa, destination_australia, other_destination_country)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)

    """
    
    
    df = df.where(pd.notna(df), None)
    
    for i, row in df.iterrows():
        values = tuple(row)
        
        
        values = tuple(None if pd.isna(v) else v for v in values)
        
        try:
            cursor.execute(query, values)
        except mysql.connector.Error as err:
            print(f"Error al insertar fila {i}: {err}")
    
    connection.commit()
    print(f"{len(df)} registros insertados")


def process_excel_files(directory_path, connection):
    for file in os.listdir(directory_path):
        if file.endswith(".xls"):
            file_path = os.path.join(directory_path, file)
            print(f"Procesando archivo: {file_path}")
            
            
            df = pd.read_excel(file_path)
            
            # Insertar los datos en la tabla
            insert_data(connection, df)
            print(f"Datos insertados desde: {file}")

 
directory_path = r'C:\Users\Diego Rodriguez\Documents\Drs\python\py2\Python24\sepomex'

connection = create_connection()


# Procesar los archivos Excel y guardar los registros en la base de datos
if connection is not None:
    process_excel_files(directory_path, connection)
    connection.close()
else:
    print("Error de conexión con MySQL")
