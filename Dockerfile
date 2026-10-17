FROM python:3.11-slim-bookworm

ARG SPARK_VERSION=3.5.5

ENV DEBIAN_FRONTEND=noninteractive \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless procps tini \
    && rm -rf /var/lib/apt/lists/* \
    && JAVA_PATH="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" \
    && ln -sfn "${JAVA_PATH}" /opt/java-home

ENV JAVA_HOME=/opt/java-home

WORKDIR /workspace

COPY requirements.txt /tmp/requirements.txt
RUN pip install --requirement /tmp/requirements.txt

# Resolve the Structured Streaming Kafka connector during the image build. The
# same Ivy cache is used by spark-submit at runtime, so startup does not depend
# on downloading the connector again.
RUN mkdir -p /opt/spark-ivy /tmp/spark-dependencies \
    && printf 'print("Spark Kafka dependencies resolved")\n' > /tmp/spark-dependencies/resolve.py \
    && spark-submit \
        --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:${SPARK_VERSION}" \
        --conf spark.jars.ivy=/opt/spark-ivy \
        /tmp/spark-dependencies/resolve.py \
    && rm -rf /tmp/spark-dependencies

RUN useradd --create-home --uid 1000 --shell /bin/bash spark \
    && mkdir -p /checkpoints \
    && chown -R spark:spark /workspace /checkpoints /opt/spark-ivy

COPY --chown=spark:spark src/ ./src/
COPY --chown=spark:spark data/ ./data/
COPY --chown=spark:spark tests/ ./tests/

USER spark

EXPOSE 4040

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "src/alerts_consumer.py"]
