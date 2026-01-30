FROM python:3.13-alpine AS builder

RUN apk add --no-cache --virtual .build-deps \
    build-base \
    postgresql-dev \
    python3-dev \
    musl-dev \
    gcc \
    libc-dev \
    linux-headers \
    zlib-dev

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY producer/pyproject.toml producer/uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

COPY producer /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync

RUN apk del .build-deps

FROM python:3.13-alpine

WORKDIR /app
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app
CMD ["faststream", "run", "main:app"]
