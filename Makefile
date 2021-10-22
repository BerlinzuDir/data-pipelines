#!make
include .env
export $(shell sed 's/=.*//' .env)

test:
	env

build:
	 DOCKER_BUILDKIT=1 docker-compose build

setup:
	docker-compose up -d

down:
	docker-compose down -v

unittest:
	docker exec airflow-scheduler pytest -v -k 'not integration' -n 'auto'

integrationtest: 
	docker exec airflow-scheduler pytest -v -k 'integration'

lint:
	docker exec airflow-scheduler poetry run flake8 
	
stylecheck:
	docker exec airflow-scheduler poetry run black --check

watch:
	docker exec -it airflow-scheduler pytest -f --ignore ./logs -k 'not integration'

watchintegration:
	docker exec -it airflow-scheduler pytest -k 'integration' -n 'auto' -f