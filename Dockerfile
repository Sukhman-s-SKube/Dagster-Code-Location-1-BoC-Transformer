FROM python:3.11-slim

ENV DAGSTER_HOME=/opt/dagster
WORKDIR ${DAGSTER_HOME}

RUN apt-get update && apt-get install -y --no-install-recommends gcc build-essential && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY boc_transformer ./boc_transformer

RUN mkdir -p ${DAGSTER_HOME}/history \
 && chown -R 1000:1000 ${DAGSTER_HOME}

RUN useradd -u 1000 -U -m dagster
USER dagster

ENTRYPOINT ["dagster", "api", "grpc", "-m", "boc_transformer.repository", "-p", "3030"]
