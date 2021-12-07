import pytest
import responses
import urllib.parse
import pandas as pd

from unittest.mock import patch

from api_wrappers.lozuka.lozuka_api import post_articles, get_articles, BASE_URL


LOGIN_DETAILS = {"username": "sample_username", "password": "sample_pw"}
TRADER_ID = 123


@responses.activate
@patch("api_wrappers.lozuka.lozuka_api.caller.transform_articles")
def test_post_articles(transform_articles_patch, _access_token) -> None:
    """Test posting articles from a dataframe to the lozuka api-endpoint."""
    transform_articles_patch.return_value = 12

    _mock_access_token_endpoint(_access_token)
    _mock_endpoint("post", "")

    post_articles(
        login_details=LOGIN_DETAILS,
        trader_id=TRADER_ID,
        articles=pd.DataFrame({"dummy": [1, 2]}),
    )
    assert len(responses.calls) == 2
    assert responses.calls[1].request.body == 12


@responses.activate
def test_get_articles(_articles, _access_token):
    _mock_access_token_endpoint(_access_token)
    _mock_endpoint("get", _articles)

    articles = get_articles(login_details=LOGIN_DETAILS, trader_id=TRADER_ID)
    assert len(responses.calls) == 2
    assert articles == {"a": "12"}


def _mock_access_token_endpoint(_access_token) -> None:
    request_url = urllib.parse.urljoin(BASE_URL, "/auth/login")
    responses.add(
        responses.POST,
        request_url,
        match_querystring=True,
        body=_access_token,
        status=200,
        match=[responses.matchers.urlencoded_params_matcher(LOGIN_DETAILS)],
    )


def _mock_endpoint(mode, _articles):
    endpoint = _get_endpoint(mode)
    request_url = urllib.parse.urljoin(BASE_URL, endpoint)
    responses_mode = responses.POST if mode == "post" else responses.GET
    responses.add(responses_mode, request_url, match_querystring=True, status=200, body=_articles)


def _get_endpoint(mode):
    assert mode in ["post", "get"], f"'{mode} not valid as input argument"
    if mode == "post":
        endpoint = f"/import/v1/articles/import?trader={TRADER_ID}&access_token=123456789"
    else:
        endpoint = f"/import/v1/articles?trader={TRADER_ID}&access_token=123456789"
    return urllib.parse.urljoin(BASE_URL, endpoint)


@pytest.fixture
def _access_token() -> bytes:
    return (
        b'["{\\"access_token\\":\\"123456789\\",'
        b'\\"expires_in\\":7200,'
        b'\\"token_type\\":\\"bearer\\",'
        b'\\"scope\\":null,'
        b'\\"refresh_token\\":\\"111\\"}"]'
    )


@pytest.fixture
def _articles() -> bytes:
    return b'{"a": "12"}'
