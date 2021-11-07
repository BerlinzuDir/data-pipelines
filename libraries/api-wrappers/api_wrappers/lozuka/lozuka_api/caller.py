import json
import pandas as pd
import requests
import urllib.parse
from returns.curry import curry

from api_wrappers.errors import ResponseError
from api_wrappers.lozuka.lozuka_api.transform import transform_articles

BASE_URL = "https://siegen.lozuka.de/"


@curry
def post_articles(login_details: dict, trader_id: int, articles: pd.DataFrame) -> None:
    request_url = _request_url(login_details, trader_id)
    articles = transform_articles(articles)
    response = requests.post(request_url, data=articles)
    response.raise_for_status()
    if response.status_code != 200:
        raise ResponseError(
            f"Posting Articles to {request_url} resulted in status code {response.status_code}."
        )


def _request_url(login_details, trader_id: int):
    endpoint = f"/import/v1/articles/import?trader={trader_id}&access_token={_access_token(login_details)}"
    return urllib.parse.urljoin(BASE_URL, endpoint)


def _access_token(login_details) -> str:
    request_url = urllib.parse.urljoin(BASE_URL, "/auth/login")
    response = requests.post(request_url, data=login_details)
    return json.loads(json.loads(response.content)[0])["access_token"]
