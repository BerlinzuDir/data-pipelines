from .dag import load_config_files


def test_load_config_files():
    configs = load_config_files("store_configs")
    assert len(configs) == 2
    assert type(configs[0]) == dict
    assert configs[0]["id"] == 287
