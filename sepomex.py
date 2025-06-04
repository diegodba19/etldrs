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
            user='UserDB',
            password='Password',
            database='sepomex'
        )
        print("Conexión exitosa a MySQL")
    except Error as e:
        print(f"Error: '{e}'")
        
    return connection





def insert_data(connection, df):
    cursor = connection.cursor()
    query = """
        INSERT INTO layout_postal_codes 
        (d_codigo, d_asenta, d_tipo_asenta, D_mnpio, d_estado, d_ciudad, d_CP, c_estado, c_oficina, c_CP, c_tipo_asenta, c_mnpio, id_asenta_cpcons, d_zona, c_cve_ciudad)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
