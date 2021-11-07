import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="bzdapiwrappers",
    version="0.0.1",
    author="BerlinzuDir",
    author_email="berlin@berlinzudir.de",
    description="API-wrapper as an interface to organize article import.",
    long_description=long_description,
    url="https://github.com/BerlinzuDir/api-wrappers",
    packages=setuptools.find_packages(),
)
