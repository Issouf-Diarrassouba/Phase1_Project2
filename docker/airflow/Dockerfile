# docker/airflow/Dockerfile
# Extends the official Airflow image with the project's Python dependencies
# and Java (needed if any DAG ever shells out to spark-submit directly).

FROM apache/airflow:2.9.3-python3.11

USER root

# Java is required for any local PySpark tooling invoked from Airflow tasks
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jdk-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow/project