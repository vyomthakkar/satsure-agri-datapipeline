"""
Comprehensive tests for the ingestion component.

Tests cover file discovery, schema validation, checkpoint management,
error handling, and data processing scenarios.
"""

import pytest
import json
from pathlib import Path
import pandas as pd
from unittest.mock import patch, Mock

from src.components.ingestion import ParquetIngestionComponent
from src.utils.exceptions import IngestionError
from tests.conftest import create_test_parquet_file


class TestParquetIngestionComponent:
    """Test suite for ParquetIngestionComponent."""

    def test_init(self, sample_config):
        """Test component initialization."""
        component = ParquetIngestionComponent(sample_config)

        assert component.config == sample_config
        assert component.duckdb_conn is not None
        assert component.logger is not None
        assert component.stats["files_discovered"] == 0
        assert component.stats["files_processed"] == 0

    def test_execute_with_sample_data(self, sample_config, sample_parquet_files):
        """Test successful execution with valid sample data."""
        component = ParquetIngestionComponent(sample_config)

        # Execute with force_full_reload to process all files
        result = component.execute(force_full_reload=True)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 10  # 5 records per file × 2 files
        assert list(result.columns) == sample_config.data_schema.expected_columns
        assert component.stats["files_processed"] == 2
        assert component.stats["files_failed"] == 0
        assert component.stats["records_ingested"] == 10

    def test_execute_specific_file(self, sample_config, sample_parquet_files):
        """Test execution with specific file path."""
        component = ParquetIngestionComponent(sample_config)

        # Execute with specific file
        specific_file = sample_parquet_files[0]
        result = component.execute(data_path=specific_file)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5  # Only one file
        assert component.stats["files_processed"] == 1
        assert component.stats["files_discovered"] == 1

    def test_execute_incremental_mode(self, sample_config, sample_parquet_files, checkpoint_file):
        """Test incremental mode processing."""
        component = ParquetIngestionComponent(sample_config)

        # Execute in incremental mode (should skip file in checkpoint)
        result = component.execute(force_full_reload=False)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5  # Only processes 2023-06-02.parquet (not in checkpoint)
        assert component.stats["files_processed"] == 1

    def test_execute_no_files(self, sample_config, temp_dir):
        """Test execution when no Parquet files exist."""
        # Create empty raw directory
        raw_dir = Path(sample_config.paths.data_raw)
        raw_dir.mkdir(parents=True, exist_ok=True)

        component = ParquetIngestionComponent(sample_config)
        result = component.execute()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0  # Empty DataFrame
        assert component.stats["files_discovered"] == 0
        assert component.stats["files_processed"] == 0

    def test_execute_missing_raw_directory(self, sample_config):
        """Test execution when raw data directory doesn't exist."""
        component = ParquetIngestionComponent(sample_config)

        with pytest.raises(IngestionError, match="Raw data directory does not exist"):
            component.execute()

    def test_discover_files_full_mode(self, sample_config, sample_parquet_files):
        """Test file discovery in full reload mode."""
        component = ParquetIngestionComponent(sample_config)

        files = component._discover_files(use_incremental=False)

        assert len(files) == 2
        assert all(f.suffix == '.parquet' for f in files)
        assert files == sorted(files)  # Should be sorted

    def test_discover_files_incremental_mode(self, sample_config, sample_parquet_files, checkpoint_file):
        """Test file discovery in incremental mode."""
        component = ParquetIngestionComponent(sample_config)

        files = component._discover_files(use_incremental=True)

        # Should exclude files in checkpoint
        assert len(files) == 1
        assert files[0].name == "2023-06-02.parquet"

    def test_load_checkpoint_existing(self, sample_config, checkpoint_file):
        """Test loading existing checkpoint file."""
        component = ParquetIngestionComponent(sample_config)

        processed_files = component._load_checkpoint()

        assert "2023-06-01.parquet" in processed_files
        assert len(processed_files) == 1

    def test_load_checkpoint_missing(self, sample_config):
        """Test loading checkpoint when file doesn't exist."""
        component = ParquetIngestionComponent(sample_config)

        processed_files = component._load_checkpoint()

        assert processed_files == set()

    def test_load_checkpoint_corrupted(self, sample_config, temp_dir):
        """Test loading corrupted checkpoint file."""
        checkpoint_path = Path(sample_config.ingestion.checkpoint_file)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

        # Create corrupted checkpoint
        with open(checkpoint_path, 'w') as f:
            f.write("invalid json content")

        component = ParquetIngestionComponent(sample_config)
        processed_files = component._load_checkpoint()

        assert processed_files == set()  # Should return empty set

    def test_update_checkpoint(self, sample_config, sample_parquet_files):
        """Test checkpoint update functionality."""
        component = ParquetIngestionComponent(sample_config)

        # Update checkpoint with processed files
        component.stats["files_processed"] = 2
        component.stats["records_ingested"] = 10
        component._update_checkpoint(sample_parquet_files)

        # Verify checkpoint was created
        checkpoint_path = Path(sample_config.ingestion.checkpoint_file)
        assert checkpoint_path.exists()

        with open(checkpoint_path, 'r') as f:
            checkpoint_data = json.load(f)

        assert len(checkpoint_data["processed_files"]) == 2
        assert "2023-06-01.parquet" in checkpoint_data["processed_files"]
        assert "2023-06-02.parquet" in checkpoint_data["processed_files"]
        assert checkpoint_data["last_run_stats"]["files_processed"] == 2

    def test_process_file_valid(self, sample_config, sample_sensor_data, temp_dir):
        """Test processing a valid Parquet file."""
        component = ParquetIngestionComponent(sample_config)

        # Create test file
        test_file = temp_dir / "valid_test.parquet"
        create_test_parquet_file(sample_sensor_data, test_file)

        result = component._process_file(test_file)

        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert list(result.columns) == sample_config.data_schema.expected_columns

    def test_process_file_missing_columns(self, sample_config, invalid_schema_data, temp_dir):
        """Test processing file with missing required columns."""
        component = ParquetIngestionComponent(sample_config)

        # Create test file with missing columns
        test_file = temp_dir / "missing_columns.parquet"
        create_test_parquet_file(invalid_schema_data, test_file)

        result = component._process_file(test_file)

        assert result is None  # Should return None for invalid schema

    def test_process_file_extra_columns(self, sample_config, temp_dir):
        """Test processing file with extra columns."""
        component = ParquetIngestionComponent(sample_config)

        # Create data with extra columns
        data = {
            "sensor_id": ["sensor_1"],
            "timestamp": [pd.Timestamp("2023-06-01 10:00:00")],
            "reading_type": ["temperature"],
            "value": [25.5],
            "battery_level": [95.5],
            "extra_column": ["extra_data"]
        }
        df = pd.DataFrame(data)

        test_file = temp_dir / "extra_columns.parquet"
        create_test_parquet_file(df, test_file)

        result = component._process_file(test_file)

        assert result is None  # Should return None for extra columns

    def test_process_file_wrong_column_order(self, sample_config, temp_dir):
        """Test processing file with different column order (should work with warning)."""
        component = ParquetIngestionComponent(sample_config)

        # Create data with different column order
        data = {
            "timestamp": [pd.Timestamp("2023-06-01 10:00:00")],
            "sensor_id": ["sensor_1"],
            "value": [25.5],
            "reading_type": ["temperature"],
            "battery_level": [95.5]
        }
        df = pd.DataFrame(data)

        test_file = temp_dir / "reordered_columns.parquet"
        create_test_parquet_file(df, test_file)

        with patch.object(component.logger, 'warning') as mock_warning:
            result = component._process_file(test_file)

            assert result is not None  # Should still process
            assert mock_warning.called  # Should log warning

    def test_process_file_wrong_types(self, sample_config, wrong_types_data, temp_dir):
        """Test processing file with wrong column types."""
        component = ParquetIngestionComponent(sample_config)

        test_file = temp_dir / "wrong_types.parquet"
        create_test_parquet_file(wrong_types_data, test_file)

        result = component._process_file(test_file)

        assert result is None  # Should return None for type mismatches

    def test_process_file_corrupted(self, sample_config, temp_dir):
        """Test processing corrupted/unreadable file."""
        component = ParquetIngestionComponent(sample_config)

        # Create corrupted file
        test_file = temp_dir / "corrupted.parquet"
        test_file.parent.mkdir(parents=True, exist_ok=True)
        with open(test_file, 'w') as f:
            f.write("This is not a valid parquet file")

        result = component._process_file(test_file)

        assert result is None  # Should return None for corrupted files

    def test_log_ingestion_summary(self, sample_config):
        """Test ingestion summary logging."""
        component = ParquetIngestionComponent(sample_config)

        # Set some stats
        component.stats.update({
            "files_discovered": 5,
            "files_processed": 4,
            "files_failed": 1,
            "records_ingested": 100
        })

        with patch.object(component.logger, 'info') as mock_info:
            component._log_ingestion_summary()

            # Should log summary and success rate
            assert mock_info.call_count >= 6  # Stats + success rate

            # Check success rate calculation
            calls = [call.args[0] for call in mock_info.call_args_list]
            success_rate_call = [call for call in calls if "Success Rate" in call]
            assert len(success_rate_call) == 1
            assert "80.0%" in success_rate_call[0]

    def test_force_full_reload_override(self, sample_config, sample_parquet_files, checkpoint_file):
        """Test that force_full_reload overrides incremental mode."""
        component = ParquetIngestionComponent(sample_config)

        # Config has incremental_mode=True, but force_full_reload should override
        result = component.execute(force_full_reload=True)

        assert len(result) == 10  # Should process both files, ignoring checkpoint
        assert component.stats["files_processed"] == 2

    def test_error_isolation(self, sample_config, temp_dir):
        """Test that processing continues even when some files fail."""
        component = ParquetIngestionComponent(sample_config)

        # Create raw directory
        raw_dir = Path(sample_config.paths.data_raw)
        raw_dir.mkdir(parents=True, exist_ok=True)

        # Create one valid file
        valid_data = pd.DataFrame({
            "sensor_id": ["sensor_1"],
            "timestamp": [pd.Timestamp("2023-06-01 10:00:00")],
            "reading_type": ["temperature"],
            "value": [25.5],
            "battery_level": [95.5]
        })
        valid_file = raw_dir / "valid.parquet"
        create_test_parquet_file(valid_data, valid_file)

        # Create one corrupted file
        corrupted_file = raw_dir / "corrupted.parquet"
        with open(corrupted_file, 'w') as f:
            f.write("corrupted content")

        result = component.execute(force_full_reload=True)

        assert len(result) == 1  # Should process the valid file
        assert component.stats["files_processed"] == 1
        assert component.stats["files_failed"] == 1