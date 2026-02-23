# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.11.11
FROM python:${PYTHON_VERSION}-slim AS base

ENV PYTHONUNBUFFERED=1
WORKDIR /app

ARG UID=10001
RUN adduser \
  --disabled-password \
  --gecos "" \
  --home "/home/appuser" \
  --shell "/bin/bash" \
  --uid "${UID}" \
  appuser

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
  postgresql libpq-dev \
  python3-dev build-essential gfortran \
  curl \
  && rm -rf /var/lib/apt/lists/*
RUN pg_config --version

ENV SETUPTOOLS_USE_DISTUTILS=local

ENV UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache/uv \
  --mount=type=bind,source=uv.lock,target=/app/uv.lock \
  --mount=type=bind,source=pyproject.toml,target=/app/pyproject.toml \
  uv sync --locked --no-install-project

COPY . /app

RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --locked

RUN mkdir -p /app/shared && chown -R appuser:appuser /app /home/appuser
USER appuser

ENV PYTHONPATH=app
ENV PATH="/app/.venv/bin:${PATH}"

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "api.app_main:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers"]