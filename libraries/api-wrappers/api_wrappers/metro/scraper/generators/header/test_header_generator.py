from header_generator import generate_header


def test_generate_header():
    header = generate_header(seed=1)
    assert (
        header ==
        {'User-Agent': 'Mozilla/5.0 (Windows NT 5.1; U; zh-cn; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50'}
    )
