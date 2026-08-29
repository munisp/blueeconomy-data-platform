# syntax=docker/dockerfile:1
# Production image for the Blue Economy data platform.
#
# Base image: python:3.12-slim, pinned by exact tag. The registry digest was
# not resolvable from the build environment used to author this file
# (auth.docker.io unreachable); pin to
# python:3.12-slim@sha256:<digest> at the next governed dependency review
# (resolve with: docker buildx imagetools inspect python:3.12-slim).
FROM python:3.12.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Run as an unprivileged service account; no shell, no home writes outside
# the application directory.
RUN groupadd --system --gid 10001 blueeconomy \
    && useradd --system --uid 10001 --gid blueeconomy --home /opt/blueeco --shell /usr/sbin/nologin blueeconomy

WORKDIR /opt/blueeco

# Hash-locked runtime dependency graph only; the lock file is the single
# source of truth (pip-compile --generate-hashes).
COPY requirements.lock /opt/blueeco/requirements.lock
RUN python -m pip install --require-hashes -r /opt/blueeco/requirements.lock

# Application package, contract schema, governed batch jobs and the
# Sedona mainApplicationFile the sedona-spark-jobs chart references as
# local:///opt/blueeco/jobs/vessel_trajectory_silver.py.
COPY pyproject.toml README.md /opt/blueeco/
COPY src /opt/blueeco/src
COPY schemas /opt/blueeco/schemas
COPY jobs /opt/blueeco/jobs
RUN python -m pip install --no-deps /opt/blueeco \
    && mkdir -p /opt/blueeco/geolibre \
    && chown -R blueeconomy:blueeconomy /opt/blueeco

# The geolibre WASI runtime is vendored at image-build or deploy time into
# /opt/blueeco/geolibre/geolibre-cli.wasm (bind-mounted or COPYed by the
# deployment pipeline, never downloaded at runtime in production).
ENV GEOLIBRE_WASM=/opt/blueeco/geolibre/geolibre-cli.wasm

USER blueeconomy:blueeconomy

# Default entrypoint: the governed Kafka consumer (platform/cvff/scoped
# event ingestion with mandatory DLQ). Gold assembly and the vessel
# consumer are alternate commands on the same image:
#   docker run <image> blueeconomy-gold-assembly
#   docker run <image> blueeconomy-ingest-vessels ...
#   docker run <image> blueeconomy-retention-enforce ...
ENTRYPOINT ["blueeconomy-ingest-kafka"]
CMD ["--help"]
