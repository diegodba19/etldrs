import requests
import json
import pprint
import hmac
import hashlib


class Veriff:
    API_SECRET = 'Apisecret'
    API_PASSPORT = 'Apipastport'


    def __init__(self):
        self.API_SECRET

    
    def getAttempts(self, api_session: str):
        signature = hmac.new(
            bytes(self.API_SECRET, 'latin-1'),
            msg=bytes(api_session, 'latin-1'),
            digestmod=hashlib.sha256
        ).hexdigest().upper()
        #print(signature)
        url = 'https://stationapi.veriff.com/v1/sessions/'+api_session+'/attempts'
        headers = {
            'X-AUTH-CLIENT': self.API_PASSPORT,
            'X-HMAC-SIGNATURE': signature,
            'Content-Type': 'application/json'
        }
        response = requests.request('GET', url, headers=headers)
        print('attemps')
        pprint.pprint(response.json())


        # Recorrer las verificaciones
        for verification in response['verifications']:
            created_time = verification['createdTime']
            verification_id = verification['id']
            verification_status = verification['status']
    
            print(f"Created Time: {created_time}")
            print(f"ID: {verification_id}")
            print(f"Status: {verification_status}")


    def getDecision(self, api_session: str):
        signature = hmac.new(
            bytes(self.API_SECRET, 'latin-1'),
            msg=bytes(api_session, 'latin-1'),
            digestmod=hashlib.sha256
        ).hexdigest().upper()
        #print(signature)
        url = 'https://stationapi.veriff.com/v1/sessions/'+api_session+'/decision'
        headers = {
            'X-AUTH-CLIENT': self.API_PASSPORT,
            'X-HMAC-SIGNATURE': signature,
            'Content-Type': 'application/json'
        }
        response = requests.request('GET', url, headers=headers)
        print('decision')
        pprint.pprint(response.json())


    def getPerson(self, api_session: str):
        signature = hmac.new(
            bytes(self.API_SECRET, 'latin-1'),
            msg=bytes(api_session, 'latin-1'),
            digestmod=hashlib.sha256
        ).hexdigest().upper()
        #print(signature)
        url = 'https://stationapi.veriff.com/v1/sessions/'+api_session+'/person'
        headers = {
            'X-AUTH-CLIENT': self.API_PASSPORT,
            'X-HMAC-SIGNATURE': signature,
            'Content-Type': 'application/json'
        }
        response = requests.request('GET', url, headers=headers)
        print('person')
        pprint.pprint(response.json())


    def getMedia(self, api_session: str):
        signature = hmac.new(
            bytes(self.API_SECRET, 'latin-1'),
            msg=bytes(api_session, 'latin-1'),
            digestmod=hashlib.sha256
        ).hexdigest().upper()
        #print(signature)
        url = 'https://stationapi.veriff.com/v1/sessions/'+api_session+'/media'
        headers = {
            'X-AUTH-CLIENT': self.API_PASSPORT,
            'X-HMAC-SIGNATURE': signature,
            'Content-Type': 'application/json'
        }
        response = requests.request('GET', url, headers=headers)
        print('media')
        pprint.pprint(response.json())


    def getWatchList(self, api_session: str):
        signature = hmac.new(
            bytes(self.API_SECRET, 'latin-1'),
            msg=bytes(api_session, 'latin-1'),
            digestmod=hashlib.sha256
        ).hexdigest().upper()
        #print(signature)
        url = 'https://stationapi.veriff.com/v1/sessions/'+api_session+'/watchlist-screening'
        headers = {
            'X-AUTH-CLIENT': self.API_PASSPORT,
            'X-HMAC-SIGNATURE': signature,
            'Content-Type': 'application/json'
        }
        response = requests.request('GET', url, headers=headers)
        print('watchlist-screening')
        pprint.pprint(response.json())

    

def main():
    uuid_session = input("Ingresa la session_id a consultar: ")
    veriff = Veriff()
    veriff.getAttempts(api_session = uuid_session)
    #veriff.getDecision(api_session = uuid_session)
    #veriff.getPerson(api_session = uuid_session)
    #veriff.getMedia(api_session = uuid_session)
    #veriff.getWatchList(api_session = uuid_session)

if __name__ == "__main__":
    exit(main())