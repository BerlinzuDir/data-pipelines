import pytest
import pandas as pd

from scraper import get_products_from_metro

STORE_ID = "0032"
CATEGORIES = ['food']
BRANDS = ["Bionade"]


@pytest.mark.block_network
@pytest.mark.vcr
def test_get_products_from_metro():
    products_df = get_products_from_metro(store_id=STORE_ID, categories=CATEGORIES, brands=BRANDS)
    assert len(products_df) == 9
    pd.testing.assert_frame_equal(products_df[1:3], _expected_dataframe(), check_like=True)


def _expected_dataframe():
    return pd.DataFrame(
        {
            "Titel": [
                "Bionade Hollunder Glas - 12 x 0,33 l Kästen",
                "Bionade Zitrone naturtrüb Glas - 12 x 0,33 l Kästen",
            ],
            "Beschreibung": ["", ""],
            "Bruttopreis": [10.06, 12.17],
            "Mehrwertsteuer": [19] * 2,
            "Maßeinheit": ["stk"] * 2,
            "Verpackungsgröße": ["12"] * 2,
            "Kategorie": ["Food / Getränke / Alkoholfreie Getränke / Erfrischungsgetränke"] * 2,
            "Produktbild": [
                "https://cdn.metro-group.com/de/de_pim_353252001002_01.png?format=jpg&quality=80&dpi=72",
                "https://cdn.metro-group.com/de/de_pim_367333001002_01.png?format=jpg&quality=80&dpi=72",
            ],
        },
        index=[1, 2],
    )
