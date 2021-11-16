import pandas as pd
import pytest

from api_wrappers.generic.sheets import get_product_data_from_sheets

TEST_URL = 'https://catalog.stolitschniy.shop/private/2VNgFokABP/vendors/berlinzudir/export'


@pytest.mark.block_network
@pytest.mark.vcr
def test_get_product_data_from_sheet():
    product_data_df = get_product_data_from_sheets(TEST_URL)
    assert len(product_data_df) == 322
    pd.testing.assert_frame_equal(product_data_df[1:3], expected_products(), check_like=True)


def expected_products() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [4600893900492, 4603514007105],
            "name": ['Aromatisierter Vodka "Green Mark Rye" 40% vol.', 'Aromatisierter Vodka "Imperial Collection Gold" 40% vol'],
            "ru name": ['Aроматизированная водка "Зеленая Марка", Ржаная" 40% алк.', 'Водка "Imperial Collection Gold" 40% алк.'],
            "price": [7.99, 11.59],
            "tax": [19] * 2,
            "weight": [0.5] * 2,
            "unit": ['l'] * 2,
            "category": ['spirituosen'] * 2,
            "tags": ['imported, online, wolt'] *2,
            "supplier": ['Monolith'] * 2,
            "pic": ['https://catalog.stolitschniy.shop/static/img/products/normalized/4600893900492.jpg', 'https://catalog.stolitschniy.shop/static/img/products/normalized/4603514007105.jpg'],
            "description": ['Enthält Weizen und Roggen.', None],
        },
        index=[1, 2],
    )
