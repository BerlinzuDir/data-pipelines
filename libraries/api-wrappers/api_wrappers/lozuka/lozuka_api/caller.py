import json
import pandas as pd
import requests
import urllib.parse
from returns.curry import curry

from api_wrappers.errors import ResponseError
from api_wrappers.lozuka.lozuka_api.transform import transform_articles

BASE_URL = "https://berlinzudir.de/"


@curry
def post_articles(login_details: dict, trader_id: int, articles: pd.DataFrame) -> None:
    request_url = _request_url("post", login_details, trader_id)
    articles = transform_articles(articles)
    response = requests.post(request_url, data=articles)
    response.raise_for_status()
    if response.status_code != 200:
        raise ResponseError(
            f"Posting Articles to {request_url} resulted in status code {response.status_code}."
        )


def get_articles(login_details: dict, trader_id: int):
    request_url = _request_url("get", login_details, trader_id)
    response = requests.get(request_url)
    return json.loads(response.content)["data"]


def deactivate_products(login_details: dict, trader_id: int, product_ids: list):
    request_url = _request_url("post", login_details, trader_id)
    request_data = {
        "data": {
            "articles": [{"itemNumber": str(pid), "active": "0"} for pid in product_ids]
        }
    }
    response = requests.post(request_url, data=json.dumps(request_data))
    response.raise_for_status()
    if response.status_code != 200:
        raise ResponseError(
            f"Deactivating Articles with {request_url} resulted in status code {response.status_code}."
        )


def _request_url(mode, login_details, trader_id: int):
    assert mode in ["post", "get"], f"'{mode} not valid as input argument"
    if mode == "post":
        endpoint = f"/import/v1/articles/import?trader={trader_id}&access_token={_access_token(login_details)}"
    else:
        endpoint = f"/import/v1/articles?trader={trader_id}&access_token={_access_token(login_details)}"
    return urllib.parse.urljoin(BASE_URL, endpoint)


def _access_token(login_details) -> str:
    request_url = urllib.parse.urljoin(BASE_URL, "/auth/login")
    response = requests.post(request_url, data=login_details)
    return json.loads(json.loads(response.content)[0])["access_token"]
