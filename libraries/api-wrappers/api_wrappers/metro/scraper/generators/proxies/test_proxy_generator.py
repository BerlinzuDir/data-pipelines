import pytest
from proxy_generator import generate_proxy


@pytest.mark.block_network
@pytest.mark.vcr
def test_generate_proxy():
    proxy = generate_proxy(seed=1)
    assert proxy == {"https": "159.146.126.143:8080"}
