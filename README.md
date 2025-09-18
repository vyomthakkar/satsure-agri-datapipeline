# satsure-agri-datapipeline
Data pipeline for SatSure agricultural sensor data.

## Docker Quickstart

Prerequisites: Docker installed and running.

### 1) Build the image

```bash
docker build -t satsure-agri:latest .
```

### 2) (Optional) Generate sample data into ./data/raw/

This uses the in-repo generator that reads `config/default.yaml` to determine directories.

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/config:/app/config" \
  satsure-agri:latest \
  python scripts/generate_synthetic_raw.py --include-edge-cases
```

### 3) Run the pipeline (default CMD)

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/reports:/app/reports" \
  -v "$(pwd)/config:/app/config" \
  satsure-agri:latest
```

Outputs:
- Processed parquet files under `./data/processed/`
- Data quality report at `./reports/data_quality_report.csv`

### 4) Run the demo variant (more verbose logging)

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/reports:/app/reports" \
  -v "$(pwd)/config:/app/config" \
  satsure-agri:latest \
  python run_pipeline_demo.py
```

### 5) (Optional) Run tests inside the container

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/config:/app/config" \
  satsure-agri:latest \
  pytest -q
```

## Volumes and configuration

- `data/` and `reports/` are mounted from the host so results persist between runs.
- Configuration is read from `config/default.yaml`. Mounting `-v "$(pwd)/config:/app/config"` lets you edit config without rebuilding.
- Default image command is `python -m src.main`. You can override the command to run other scripts:

```bash
# Run the data generator only
docker run --rm -v "$(pwd)/data:/app/data" -v "$(pwd)/config:/app/config" satsure-agri:latest \
  python scripts/generate_synthetic_raw.py --dates 2023-06-02 2023-06-03 2023-06-04

# Run the demo script
docker run --rm -v "$(pwd)/data:/app/data" -v "$(pwd)/reports:/app/reports" -v "$(pwd)/config:/app/config" satsure-agri:latest \
  python run_pipeline_demo.py
```

## Apple Silicon (M1/M2/M3) note

If you encounter wheel compatibility issues, try building for amd64:

```bash
docker build --platform linux/amd64 -t satsure-agri:latest .
```

## Troubleshooting

- Ensure the `data/` and `reports/` directories exist on your host (Docker will create them if you mount with `-v`).
- If you see permission issues on mounted volumes, ensure your user has write permissions to `./data` and `./reports`.
- Verify `config/default.yaml` paths match your mounted directories (defaults are `data/raw`, `data/processed`, `reports`).
