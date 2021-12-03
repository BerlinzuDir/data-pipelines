from unittest.mock import MagicMock

import pytest
import pandas as pd

from api_wrappers.metro.scraper import get_products_from_metro, PROXY_GENERATOR


STORE_ID = "0032"
CATEGORIES = ["food"]
BRANDS = ["Bionade"]
QUERY = "bio"


@pytest.mark.block_network
@pytest.mark.vcr
def test_get_products_from_metro():
    def side_effect(proxy_generator):
        proxy_generator.proxies = {"https": "95.111.225.137:443"}

    PROXY_GENERATOR.reset_proxy = MagicMock(side_effect=side_effect(PROXY_GENERATOR))

    products = get_products_from_metro(
        store_id=STORE_ID,
        categories=CATEGORIES,
        brands=BRANDS,
        rows=5,
        query=QUERY,
    )
    assert len(products) == 7
    pd.testing.assert_frame_equal(products.take([2, 4]), _expected_dataframe(), check_like=True)


def _expected_dataframe():
    return pd.DataFrame(
        {
            "Titel": [
                "Bionade Hollunder Glas - 12 x 0,33 l Kästen",
                "Bionade Zitrone naturtrüb Glas - 12 x 0,33 l Kästen",
            ],
            "Id": ["BTY-X35329400320022", "BTY-X36737500320022"],
            "Marke": ["BIONADE", "BIONADE"],
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
