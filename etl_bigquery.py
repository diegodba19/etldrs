import requests
def fetch_data():
    url = 'http://localhost/bff/catalogs-dashboard/unique-users?limit=1&offset=0'
    headers = {
        'accept': 'application/json',
        'x-api-key': 'Secret Key'
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()
data = fetch_data()
print(data)