#!make
include .env
export $(shell sed 's/=.*//' .env)

build:
	 DOCKER_BUILDKIT=1 docker-compose build

setup:
	docker-compose up -d

teardown:
	docker-compose down -v

unittest:
	docker exec airflow-scheduler pytest -v -k 'not integration' -n 'auto'

integrationtest: 
	docker exec airflow-scheduler pytest -v -k 'integration'

lint:
	poetry run flake8
	
stylecheck:
	docker exec airflow-scheduler poetry run black --check

watch:
	docker exec -it airflow-scheduler pytest -f --ignore ./logs -k 'not integration'

watchthis:
	docker exec -it airflow-scheduler pytest -f --ignore ./logs -k $(TEST)
