# Airflow 3.2.2 (matches requirements.txt) + Java 17 + PySpark so the
# BashOperator can run `spark-submit --master local[2]` inside this container.
FROM apache/airflow:3.2.2

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# default-jre gives an arch-agnostic symlink (works on amd64 and arm64 hosts)
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
# Pin pyspark to the same Spark version as the apache/spark:4.0.3 containers.
# This provides the `spark-submit` binary on PATH.
RUN pip install --no-cache-dir pyspark==4.0.3
