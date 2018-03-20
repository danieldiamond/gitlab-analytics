#!/usr/bin/python3

import os
import requests

mk_username = os.environ.get('MKTO_USER')
mk_endpoint = os.environ.get('MKTO_ENDPOINT')
mk_client_id = os.environ.get('MKTO_CLIENT_ID')
mk_client_secret = os.environ.get('MKTO_CLIENT_SECRET')


def get_token():

    payload = {
        "grant_type": "client_credentials",
        "client_id": mk_client_id,
        "client_secret": mk_client_secret
    }

    response = requests.get(mk_endpoint + 'identity/oauth/token', params=payload)

    if response.status_code == 200:
        r_json = response.json()
        token = r_json.get("access_token", None)
        return token
    else:
        return "Error"


if __name__ == "__main__":
    print(get_token())