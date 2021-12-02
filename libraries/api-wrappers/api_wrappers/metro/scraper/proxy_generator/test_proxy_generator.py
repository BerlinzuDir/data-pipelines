import pytest
from proxy_generator import proxy_generator


@pytest.mark.block_network
@pytest.mark.vcr
def test_proxy_generator():
    proxy = proxy_generator(seed=1)
    assert proxy == {'https': '54.37.160.90:1080'}
