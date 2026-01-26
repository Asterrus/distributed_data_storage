# FROM python:3.13-slim

# ENV PYTHONUNBUFFERED=1

# WORKDIR /app

# # Install uv
# COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# ENV PATH="/app/.venv/bin:$PATH"
# ENV UV_COMPILE_BYTECODE=1
# ENV UV_LINK_MODE=copy

# # Копируем только файлы зависимостей
# COPY producer/pyproject.toml producer/uv.lock ./

# RUN --mount=type=cache,target=/root/.cache/uv \
#     --mount=type=bind,source=uv.lock,target=uv.lock \
#     --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
#     uv sync --frozen --no-install-project

# ENV PYTHONPATH=/app

# # Копируем проект
# COPY producer /app

# RUN --mount=type=cache,target=/root/.cache/uv \
#     uv sync

# CMD ["faststream", "run", "main:app"]
FROM python:3.13-alpine AS builder

# Устанавливаем build-deps для psycopg
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

# Копируем uv из astral-sh (если нужно, иначе pip)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Зависимости
COPY producer/pyproject.toml producer/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

# Проект
COPY producer /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync

# Чистим build-deps после сборки
RUN apk del .build-deps

# Финальный образ — лёгкий, без build-мусора
FROM python:3.13-alpine

WORKDIR /app
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app
CMD ["faststream", "run", "main:app"]
