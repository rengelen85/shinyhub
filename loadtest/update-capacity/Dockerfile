FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv==0.10.12 \
    && uv pip install --system shiny==1.8.0
RUN mkdir /opt/wheels && find /root/.cache/uv -name 'libsass-*.whl' -exec cp '{}' /opt/wheels/ ';'
ENV UV_PYTHON=/usr/local/bin/python UV_PYTHON_DOWNLOADS=never
ENV UV_FIND_LINKS=/opt/wheels
