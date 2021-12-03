import pytest
import pandas as pd

from unittest.mock import patch, MagicMock
from scraper import get_products_from_metro


STORE_ID = "0032"
CATEGORIES = ["food"]
BRANDS = ["Bionade"]
QUERY = "bio"


@pytest.mark.block_network
@pytest.mark.vcr
@patch('scraper.PROXY_GENERATOR')
def test_get_products_from_metro(proxy_generator):
    proxy_generator.proxies = {'https': '95.111.225.137:443'}

    products_page_1 = get_products_from_metro(
        store_id=STORE_ID,
        categories=CATEGORIES,
        brands=BRANDS,
        rows=5,
        page=1,
        query=QUERY,
    )
    products_page_2 = get_products_from_metro(store_id=STORE_ID, categories=CATEGORIES, brands=BRANDS, rows=5, page=2)
    assert len(products_page_1) == 6
    assert len(products_page_2) == 1
    pd.testing.assert_frame_equal(products_page_1.take([2, 4]), _expected_dataframe(), check_like=True)


def _expected_dataframe():
    return pd.DataFrame(
        {
            "Titel": [
                "Bionade Hollunder Glas - 12 x 0,33 l Kästen",
                "Bionade Zitrone naturtrüb Glas - 12 x 0,33 l Kästen",
            ],
            "Beschreibung": ["", ""],
            "Bruttopreis": [12.17, 12.17],
            "Mehrwertsteuer": [19] * 2,
            "Maßeinheit": ["stk"] * 2,
            "Verpackungsgröße": ["12"] * 2,
            "Kategorie": ["Food / Getränke / Alkoholfreie Getränke / Erfrischungsgetränke"] * 2,
            "Produktbild": [
                "https://cdn.metro-group.com/de/de_pim_353252001002_01.png?format=jpg&quality=80&dpi=72",
                "https://cdn.metro-group.com/de/de_pim_367333001002_01.png?format=jpg&quality=80&dpi=72",
            ],
            "gtins/eans": [[4014472002741], [4014472980056]],
        },
        index=[2, 4],
    )
