import pytest
from api_wrappers.metro.scraper.generators.proxies.proxy_generator import ProxyGenerator


@pytest.mark.block_network
@pytest.mark.vcr
def test_generate_proxy():
    proxy = ProxyGenerator()
    proxy.reset_proxy(seed=1)
    assert proxy.proxies == {"https": "159.146.126.143:8080"}
