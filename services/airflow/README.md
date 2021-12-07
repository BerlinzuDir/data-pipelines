# data-pipelines

Data pipelines and infrastructure to manage product data at [BerlinzuDir](https://berlinzudir.de)

Written in python, built with [Airflow](https://airflow.apache.org/) and Docker.

This project uses [poetry](https://python-poetry.org/) for package management and [git-secret](https://git-secret.io/) to manage secrets.

## How to work with this project:

Prerequisites: 
1. Set up [docker](https://docs.docker.com/get-docker/) and [docker-compose](https://docs.docker.com/compose/install/) for your operating system
2. Set up [git secret](https://git-secret.io/installation)
3. Contact @jakobkolb to get accesss to repository secrets

Usage:
1. Clone the project
2. Install poetry: `$pip install --user poetry`
3. run git-secret reveal
4. Run `make build` then `make  setup`
5. Run tests: `make unittests` 
6. `$make watch` or open the Airflow UI on `localhost:8080` and login with username: `airflow` and password: `airflow`
