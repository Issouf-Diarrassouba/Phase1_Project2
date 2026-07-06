# docker/spark/Dockerfile
# Bitnami's Spark image with the project's Python dependencies layered on,
# so spark-submit jobs can `import kafka`, `import faker`, etc. if needed
# for local testing utilities.

FROM bitnamilegacy/spark:3.5.1

USER root

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Config picked up automatically by spark-submit via SPARK_CONF_DIR
ENV SPARK_CONF_DIR=/opt/spark-config

WORKDIR /opt/spark/project