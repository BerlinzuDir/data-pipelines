import json
import pathlib
from typing import TypedDict
import ramda as R
import requests


class OrdersAuthCredentials(TypedDict):
    grant_type: str
    client_id: str
    client_secret: str
    username: str
    password: str


class OrdersAPIToken(TypedDict):
    access_token: str


authenticate = lambda *_: R.pipe(get_auth_context, get_delivery_api_auth_token)("/delivery_api_credentials.json")


def get_delivery_api_auth_token(auth_context: OrdersAuthCredentials) -> OrdersAPIToken:
    res = requests.post("https://siegen.lozuka.de/oauth/v2/token", data=auth_context)
    return json.loads(res.text)


def get_auth_context(auth_file: str) -> OrdersAuthCredentials:
    with open(_get_path_of_file() + auth_file) as f:
        auth_context = json.load(f)
    return auth_context


def _get_path_of_file() -> str:
    return str(pathlib.Path(__file__).parent.resolve())
