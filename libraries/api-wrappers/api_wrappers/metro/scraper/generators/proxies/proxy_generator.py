import json
import os


def get_proxy() -> dict:
    credentials = _get_credentials()
    proxy = f'http://{credentials["username"]}:{credentials["password"]}@de.smartproxy.com:20000'
    return {'http': proxy, 'https': proxy}


def _get_credentials() -> dict:
    dir_path = os.path.dirname(os.path.realpath(__file__))
    credentials_file = os.path.join(dir_path, 'smartproxy_credentials.json')
    with open(credentials_file, 'r') as credentials:
        return json.load(credentials)
