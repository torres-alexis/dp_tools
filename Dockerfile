FROM python:3.11-slim-bookworm

ARG DEBIAN_FRONTEND=noninteractive

RUN groupadd -r genuser && \
    useradd -g genuser genuser && \
    mkdir /home/genuser && \
    chmod -R 777 /home/genuser && \
    apt-get update && \
    apt-get install -y --no-install-recommends curl samtools zip && \
    rm -rf /var/lib/apt/lists/*

COPY . /app

RUN chown -R genuser:genuser /app && \
    pip install --ignore-installed PyYAML /app && \
    rm -rf /app

ENV PATH=/home/genuser/.local/bin:$PATH

USER genuser

WORKDIR /home/genuser
