FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AFTERTAKE_OUT_DIR=/data/out

WORKDIR /app

RUN mkdir -p /data/out

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir ".[live]"

STOPSIGNAL SIGTERM

CMD ["python", "-m", "aftertake.runner", "--forever"]
