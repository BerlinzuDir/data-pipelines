import ipaddress

import requests
from bs4 import BeautifulSoup
import random


class ProxyGenerator:
    def __init__(self):
        self.proxies = None

    def reset_proxy(self, seed=None):
        if seed:
            random.seed(seed)
        response = requests.get("https://sslproxies.org/")
        soup = BeautifulSoup(response.content, "html.parser")
        proxy_ips = map(lambda x: x.text, soup.findAll("td")[::8])
        proxy_ports = map(lambda x: x.text, soup.findAll("td")[1::8])
        proxy_ips_ports = list(map(lambda x: x[0] + ":" + x[1], list(zip(proxy_ips, proxy_ports))))
        while True:
            proxy = {"https": random.choice(proxy_ips_ports)}
            try:
                ipaddress.ip_address(proxy["https"].split(":")[0])
                self.proxies = proxy
                break
            except ValueError as error:
                print(error)
