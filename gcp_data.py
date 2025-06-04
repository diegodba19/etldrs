import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict

def fetch_data():
    url = 'http://localhost/bff/catalogs-dashboard/unique-users?limit=2000&offset=0'
    headers = {
        'accept': 'application/json',
        'x-api-key': 'secret Key'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

# Configura tus credenciales
credentials = service_account.Credentials.from_service_account_file(
    'datapath_etl.json'
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id)

project_id = 'general-dashboard-prod'
dataset_id = 'dwh_sendola'
table_id = 'v_unique_users'
temp_table_id = 'v_unique_users_temp'

# Crea una referencia al dataset
dataset_ref = client.dataset(dataset_id, project=project_id)

# Define el esquema de la tabla
schema = [
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("person_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("full_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("middle_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("phone", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country_origin", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("nationality", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("date_of_birth", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("monthly_payroll", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("last_payroll_received_at", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("kyc_status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("kyc_approved_date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("solid_accounts", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("usi_accounts", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("banner_accounts", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("coppel_accounts", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("plaid_accounts", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("penwheel_accounts", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("customer_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("customer", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("invitation_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("is_one", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("is_gpm", "BOOLEAN", mode="NULLABLE"),
    bigquery.SchemaField("application", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("coppel_access_status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("coppel_access_status_at", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("coppel_external_status", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dd_percentage", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("dd_form_accepted", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dd_accepted_at", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("dd_form_active_feature", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("modified_at", "TIMESTAMP", mode="NULLABLE"),
]

# Crear la tabla temporal
temp_table_ref = dataset_ref.table(temp_table_id)
table_ref = dataset_ref.table(table_id)
print(temp_table_ref)

try:
    temp_table = client.create_table(bigquery.Table(temp_table_ref, schema=schema))
    print(f"Tabla temporal {temp_table_id} creada en el dataset {dataset_id}.")
except Conflict:
    print(f"La tabla temporal {temp_table_id} ya existe en el dataset {dataset_id}.")

# Insertar los nuevos datos en la tabla temporal
data = fetch_data()
results = data['data']['results']

errors = client.insert_rows_json(temp_table_ref, results)
if not errors:
    print("Datos insertados correctamente en la tabla temporal.")
else:
    print(f"Errores al insertar en la tabla temporal: {errors}")

# Reemplazar la tabla original con la tabla temporal
query = f"""
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{table_id}`
AS SELECT * FROM `{project_id}.{dataset_id}.{temp_table_id}`
"""
client.query(query).result()
print(f"Tabla {table_id} reemplazada con los datos nuevos.")

# Borrar la tabla temporal
client.delete_table(temp_table_ref)
print(f"Tabla temporal {temp_table_id} eliminada.")
