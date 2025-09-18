# syntax=docker/dockerfile:1

# Base image: slim Python with good wheel support for pandas/pyarrow/duckdb/scipy
FROM python:3.11-slim

# Environment setup
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Working directory inside container
WORKDIR /app

# Install Python dependencies first (leverages build cache)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . /app

# Default command runs the pipeline orchestrator
CMD ["python", "-m", "src.main"]
