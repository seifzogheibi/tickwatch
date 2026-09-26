FROM python:3.13-slim

# No .pyc files in the image; unbuffered logs so `docker compose logs` is live.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first, so code changes don't reinstall numpy/sklearn.
COPY pyproject.toml ./
RUN mkdir -p src/tickwatch && touch src/tickwatch/__init__.py \
    && pip install . && pip uninstall -y tickwatch

COPY src ./src
RUN pip install --no-deps .

RUN useradd --system --uid 10001 --create-home tickwatch \
    && mkdir -p /data/raw && chown -R tickwatch /data
USER tickwatch

# Apply the (idempotent) schema, then run the consumer as PID 1 so it gets
# SIGTERM directly from `docker stop` and shuts down cleanly.
CMD ["sh", "-c", "python -m tickwatch.db init && exec tickwatch"]
