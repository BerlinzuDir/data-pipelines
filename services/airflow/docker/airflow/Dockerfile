FROM apache/airflow:2.2.1-python3.8

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

RUN groupadd airflow && \
    usermod -a -G airflow airflow && \
    chown -R airflow:airflow /usr/local/src

USER airflow

ARG YOUR_ENV

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    POETRY_VERSION=1.1.11

RUN pip install "poetry==$POETRY_VERSION"

RUN python3 --version

WORKDIR /opt/airflow/

ADD services/airflow/airflow.cfg services/airflow/poetry.lock services/airflow/pyproject.toml /opt/airflow/

# copy libs into the container, so their paths exist for editable installs
COPY libraries ../../libraries

RUN pip install --requirement <(poetry export  --without-hashes --dev --format requirements.txt)

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/dags"