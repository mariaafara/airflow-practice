ARG AIRFLOW_VERSION=2.7.1

FROM apache/airflow:${AIRFLOW_VERSION}-python3.11
WORKDIR /airflow
COPY requirements.txt /airflow/requirements.txt
RUN pip install -r /airflow/requirements.txt
