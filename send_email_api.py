
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

def send_email_via_api(table):
    url = "http://localhost:9100/notifications/sent-emailattachment"
    headers = {
        "Authorization": "Apikey",  # Reemplaza con tu API Key real
        "accept": "text/plain",
    }

    email_settings = ["diego.rodriguez@sendola.io",]


    fields = {
        'FromEmail': 'noreply@sendola.dev',
        'Subject': 'Test DEV Info',
        'TextBody': 'Cuerpo Correo',
        'HtmlBody': table,
        'Atachment': '',
        'MessageTypeName': ''
    }

    for i, email in enumerate(email_settings):
        fields[f'ToEmail[{i}]'] = email

    m = MultipartEncoder(fields=fields)
    headers['Content-Type'] = m.content_type

    response = requests.post(url, headers=headers, data=m)

    if response.status_code == 200:
        print("Correo enviado exitosamente.")
    else:
        print(f"Error al enviar el correo: {response.status_code} - {response.text}")

