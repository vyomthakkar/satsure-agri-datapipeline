"""
Pytest configuration and shared fixtures for testing.

Provides common test fixtures and setup for all test modules.
"""

import tempfile
import pytest
from pathlib import Path
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import json

from src.config import PipelineConfig


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


@pytest.fixture
def sample_config(temp_dir):
    """Create a test configuration with temporary paths."""
    config_data = {
        "pipeline": {
            "name": "test_agricultural_sensor_pipeline",
            "version": "1.0.0"
        },
        "project": {
            "timezone": "UTC+05:30",
            "run_id": None
        },
        "paths": {
            "data_raw": str(temp_dir / "raw"),
            "data_processed": str(temp_dir / "processed"),
            "reports_dir": str(temp_dir / "reports"),
            "dq_report_csv": str(temp_dir / "reports" / "data_quality_report.csv")
        },
        "schema": {
            "expected_columns": ["sensor_id", "timestamp", "reading_type", "value", "battery_level"],
            "types": {
                "sensor_id": "VARCHAR",
                "timestamp": "TIMESTAMP",
                "reading_type": "VARCHAR",
                "value": "DOUBLE",
                "battery_level": "DOUBLE"
            }
        },
        "ranges": {
            "temperature": {"min": -10, "max": 60},
            "humidity": {"min": 0, "max": 100},
            "battery_level": {"min": 0, "max": 100}
        },
        "calibration": {
            "temperature": {"multiplier": 1.0, "offset": 0.0},
            "humidity": {"multiplier": 1.0, "offset": 0.0}
        },
        "write": {
            "compression": "zstd",
            "partition_by": ["date", "sensor_id"],
            "mode": "overwrite"
        },
        "transformation": {
            "z_score_threshold": 3.0,
            "rolling_window_days": 7,
            "outlier_handling": "flag"
        },
        "validation": {
            "max_missing_percentage": 20,
            "expected_frequency_hours": 1
        },
        "ingestion": {
            "incremental_mode": True,
            "checkpoint_file": str(temp_dir / ".checkpoint")
        }
    }

    return PipelineConfig(**config_data)


@pytest.fixture
def sample_sensor_data():
    """Create sample sensor data for testing."""
    data = {
        "sensor_id": ["sensor_1", "sensor_1", "sensor_2", "sensor_2", "sensor_3"],
        "timestamp": [
            datetime(2023, 6, 1, 10, 0, 0),
            datetime(2023, 6, 1, 10, 30, 0),
            datetime(2023, 6, 1, 10, 15, 0),
            datetime(2023, 6, 1, 10, 45, 0),
            datetime(2023, 6, 1, 11, 0, 0)
        ],
        "reading_type": ["temperature", "humidity", "temperature", "humidity", "temperature"],
        "value": [25.5, 65.2, 24.8, 68.1, 26.2],
        "battery_level": [95.5, 95.0, 87.3, 86.8, 92.1]
    }
    return pd.DataFrame(data)


@pytest.fixture
def invalid_schema_data():
    """Create data with invalid schema for testing."""
    data = {
        "sensor_id": ["sensor_1", "sensor_2"],
        "timestamp": [datetime(2023, 6, 1, 10, 0, 0), datetime(2023, 6, 1, 10, 30, 0)],
        "reading_type": ["temperature", "humidity"],
        "value": [25.5, 65.2],
        # Missing battery_level column
        "extra_column": ["extra1", "extra2"]  # Extra column
    }
    return pd.DataFrame(data)


@pytest.fixture
def wrong_types_data():
    """Create data with wrong column types for testing."""
    data = {
        "sensor_id": ["sensor_1", "sensor_2"],
        "timestamp": ["2023-06-01 10:00:00", "2023-06-01 10:30:00"],  # String instead of timestamp
        "reading_type": ["temperature", "humidity"],
        "value": ["25.5", "65.2"],  # String instead of float
        "battery_level": [95.5, 68.1]
    }
    return pd.DataFrame(data)


def create_test_parquet_file(data: pd.DataFrame, file_path: Path) -> None:
    """
    Create a test Parquet file from DataFrame.

    Args:
        data: DataFrame to save
        file_path: Path where to save the file
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    # Convert to PyArrow table for precise type control
    if 'timestamp' in data.columns and data['timestamp'].dtype == 'object':
        # Handle string timestamps
        data = data.copy()
        data['timestamp'] = pd.to_datetime(data['timestamp'])

    table = pa.Table.from_pandas(data)
    pq.write_table(table, file_path)


@pytest.fixture
def sample_parquet_files(temp_dir, sample_config, sample_sensor_data):
    """Create sample Parquet files in the test raw data directory."""
    raw_dir = Path(sample_config.paths.data_raw)
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Create multiple test files
    files = []

    # File 1: Valid data
    file1 = raw_dir / "2023-06-01.parquet"
    create_test_parquet_file(sample_sensor_data, file1)
    files.append(file1)

    # File 2: More valid data (different date)
    file2 = raw_dir / "2023-06-02.parquet"
    data2 = sample_sensor_data.copy()
    data2['timestamp'] = data2['timestamp'].apply(lambda x: x.replace(day=2))
    create_test_parquet_file(data2, file2)
    files.append(file2)

    return files


@pytest.fixture
def checkpoint_file(sample_config):
    """Create a test checkpoint file."""
    checkpoint_path = Path(sample_config.ingestion.checkpoint_file)
    checkpoint_data = {
        "processed_files": ["2023-06-01.parquet"],
        "last_update": "2023-06-02T10:00:00",
        "last_run_stats": {
            "files_processed": 1,
            "records_ingested": 5
        }
    }

    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    with open(checkpoint_path, 'w') as f:
        json.dump(checkpoint_data, f)

    return checkpoint_path