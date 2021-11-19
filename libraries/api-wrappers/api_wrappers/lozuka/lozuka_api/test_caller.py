import urllib.parse
from unittest.mock import patch

import pandas as pd
import responses

from api_wrappers.lozuka.lozuka_api import post_articles


LOGIN_DETAILS = {"username": "sample_username", "password": "sample_pw"}
TRADER_ID = 123
BASE_URL = "https://siegen.lozuka.de/"


@responses.activate
@patch("api_wrappers.lozuka.lozuka_api.caller.transform_articles")
def test_post_articles(transform_articles_patch) -> None:
    """Test posting articles from a dataframe to the lozuka api-endpoint."""
    transform_articles_patch.return_value = 12
    _setup_request_mocks()
    post_articles(
        login_details=LOGIN_DETAILS,
        trader_id=TRADER_ID,
        articles=pd.DataFrame({"dummy": [1, 2]}),
    )
    assert len(responses.calls) == 2
    assert (
        responses.calls[0].request.body == f'username={LOGIN_DETAILS["username"]}&password={LOGIN_DETAILS["password"]}'
    )
    assert responses.calls[1].request.body == 12


def _setup_request_mocks() -> None:
    _mock_access_token_endpoint()
    _mock_post_articles_endpoint(TRADER_ID)


def _mock_access_token_endpoint() -> None:
    request_url = urllib.parse.urljoin(BASE_URL, "/auth/login")
    responses.add(
        responses.POST,
        request_url,
        match_querystring=True,
        body=_access_token(),
        status=200,
    )


def _mock_post_articles_endpoint(trader_id: int) -> None:
    endpoint = f"/import/v1/articles/import?trader={trader_id}&access_token={123456789}"
    request_url = urllib.parse.urljoin(BASE_URL, endpoint)
    responses.add(responses.POST, request_url, match_querystring=True, status=200)


def _access_token() -> bytes:
    return (
        b'["{\\"access_token\\":\\"123456789\\",'
        b'\\"expires_in\\":7200,'
        b'\\"token_type\\":\\"bearer\\",'
        b'\\"scope\\":null,'
        b'\\"refresh_token\\":\\"111\\"}"]'
    )
