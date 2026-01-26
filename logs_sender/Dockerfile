FROM python:3.13-alpine AS builder

WORKDIR /app
ENV PYTHONUNBUFFERED=1
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

COPY logs_sender/pyproject.toml logs_sender/uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project

COPY logs_sender /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync

FROM python:3.13-alpine

WORKDIR /app
COPY --from=builder /app /app
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH=/app
CMD ["python", "main.py"]
