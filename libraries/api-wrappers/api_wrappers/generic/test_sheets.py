import responses
from api_wrappers.generic.sheets import get_product_data_from_sheets

TEST_URL = 'https://test_url.de/export'


@responses.activate
def test_get_product_data_from_sheet():
    _mock_csv_endpoint()

    teest_df = get_product_data_from_sheets(TEST_URL)
    import pdb;pdb.set_trace()


def _mock_csv_endpoint():
    responses.add(
        responses.GET,
        TEST_URL,
        match_querystring=True,
        content_type='text/csv',
        headers={'content-disposition': 'attachment; filename=export.csv'},
        body=_response_body(),
        status=200,
        stream=True,
    )


def _response_body():
    return bytes(
        'id,name,price,tax,weight,unit\r\n'
        '2, bla1, 1.59, 7, 480.0 , g\r\n'
        '3, bla2, 1.59, 7, 480.0 , g\r\n'
        '4, bla3, 1.59, 7, 480.0 , g\r\n'
    )
