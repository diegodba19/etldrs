import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict, NotFound

def mask_account_number(account_number):
    if len(account_number) > 4:
        return '*' * (len(account_number) - 4) + account_number[-4:]
    return account_number

def get_transactions():
    url = 'http:/localhost/bff/catalogs-dashboard/transactions?limit=50000&offset=0'
    headers = {
        'accept': 'application/json',
        'x-api-key': 'ZDg4NGZmZmMtNGJkZS00ZjlkLWJkYTgtMDZlNjkwMjFlMjMx'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def table_exists(client, dataset_id, table_id):
    try:
        client.get_table(f"{dataset_id}.{table_id}")
        return True
    except NotFound:
        return False

def get_bigquery_record_count(client, dataset_id, table_id):
    query = f"SELECT COUNT(*) as total FROM `{dataset_id}.{table_id}`"
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        return row.total

def main():
    # Configuración de credenciales y cliente
    credentials = service_account.Credentials.from_service_account_file(
        'datapath_etl.json'
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    dataset_id = 'dwh_sendola'
    table_id = 'transaction_plattform'
    temp_table_id = 'transaction_plattform_temp'

    # Valido si la tabla original existe
    if not table_exists(client, dataset_id, table_id):
        print(f"La tabla original {table_id} no existe en el dataset {dataset_id}.")
        return

    data = get_transactions()
    total_from_endpoint = data['data']['total']
    total_in_table = get_bigquery_record_count(client, dataset_id, table_id)

    if total_from_endpoint != total_in_table:
        # Valido si la tabla temporal existe y eliminarla
        temp_table_ref = f"{dataset_id}.{temp_table_id}"
        try:
            client.delete_table(temp_table_ref)
            print(f"Tabla temporal {temp_table_id} eliminada antes de recrearla.")
        except NotFound:
            print(f"La tabla temporal {temp_table_id} no existe, se creará una nueva.")

        # Creo una referencia al dataset
        dataset_ref = client.dataset(dataset_id, project=credentials.project_id)

        # Creo la tabla temporal
        schema = [
            bigquery.SchemaField("transaction_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("person_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("account_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("amount", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("balance", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("label", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("account_number", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("account_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("transfer_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("txn_type", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
        ]

        # Crear la tabla temporal
        temp_table_ref = dataset_ref.table(temp_table_id)
        try:
            client.create_table(bigquery.Table(temp_table_ref, schema=schema))
            print(f"Tabla temporal {temp_table_id} creada en el dataset {dataset_id}.")
        except Conflict:
            print(f"La tabla temporal {temp_table_id} ya existe en el dataset {dataset_id}.")

        # Insertar los nuevos datos en la tabla temporal en lotes
        results = data['data']['results']       

        if results:
            batch_size = 5000  # Tamaño del lote
            for i in range(0, len(results), batch_size):
                batch = results[i:i + batch_size]
                errors = client.load_table_from_json(batch, temp_table_ref).result()
                if errors:
                    print(f"Errores al insertar en la tabla temporal: {errors}")
                else:
                    print(f"Datos insertados correctamente en la tabla temporal (lote {i // batch_size + 1}).")
        else:
            print(f"No se encontraron datos para insertar en la tabla temporal.")

        # Borrar datos en la tabla original y copiar los nuevos datos
        client.query(f"DELETE FROM `{dataset_id}.{table_id}` WHERE TRUE").result()
        client.query(f"""
            INSERT INTO `{dataset_id}.{table_id}`
            SELECT * FROM `{dataset_id}.{temp_table_id}`
        """).result()

        # Eliminar la tabla temporal
        client.delete_table(temp_table_ref)
        print(f"Tabla temporal {temp_table_id} eliminada.")

        # Realizar el UPDATE para modificar el account_number
        update_query = f"""
            UPDATE `{dataset_id}.{table_id}`
            SET account_number = CONCAT(REPEAT('*', LENGTH(account_number) - 4), RIGHT(account_number, 4))
            WHERE account_number IS NOT NULL
        """
        client.query(update_query).result()
        print(f"UPDATE ejecutado correctamente en la tabla {table_id}.")
    else:
        print("No hay cambios en el número de registros, no se realizó ninguna actualización.")

if __name__ == "__main__":
    main()