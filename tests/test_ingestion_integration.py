"""
Integration tests for the ingestion component using real sample data.

These tests verify that the ingestion component works correctly with
the actual sample data file provided in the project.
"""

import pytest
from pathlib import Path
import pandas as pd

from src.components.ingestion import ParquetIngestionComponent
from src.config import PipelineConfig


class TestIngestionIntegration:
    """Integration tests using real sample data."""

    @pytest.fixture
    def real_config(self):
        """Load the actual project configuration."""
        config_path = Path("config/default.yaml")
        if not config_path.exists():
            pytest.skip("Real config file not found - integration test requires actual project setup")

        return PipelineConfig.from_yaml(config_path)

    @pytest.fixture
    def real_sample_file(self, real_config):
        """Check if real sample data file exists."""
        raw_data_path = Path(real_config.paths.data_raw)
        sample_files = list(raw_data_path.glob("*.parquet"))

        if not sample_files:
            pytest.skip("No real sample parquet files found - cannot run integration tests")

        return sample_files[0]  # Use first available file

    def test_real_data_processing(self, real_config, real_sample_file):
        """Test processing real sample data from the project."""
        component = ParquetIngestionComponent(real_config)

        # Process the real sample file
        result = component.execute(data_path=real_sample_file)

        # Verify results
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0, "Sample file should contain data"

        # Verify schema matches expectations
        expected_columns = real_config.data_schema.expected_columns
        assert list(result.columns) == expected_columns

        # Verify data types are reasonable
        assert result['sensor_id'].dtype == 'object'
        assert pd.api.types.is_datetime64_any_dtype(result['timestamp'])
        assert result['reading_type'].dtype == 'object'
        assert pd.api.types.is_numeric_dtype(result['value'])
        assert pd.api.types.is_numeric_dtype(result['battery_level'])

        # Log what we found for verification
        print(f"\\nProcessed real sample data:")
        print(f"File: {real_sample_file}")
        print(f"Records: {len(result)}")
        print(f"Sensors: {result['sensor_id'].unique()}")
        print(f"Reading types: {result['reading_type'].unique()}")
        print(f"Date range: {result['timestamp'].min()} to {result['timestamp'].max()}")

    def test_real_data_statistics(self, real_config, real_sample_file):
        """Test that statistics are correctly calculated for real data."""
        component = ParquetIngestionComponent(real_config)

        result = component.execute(data_path=real_sample_file)

        # Check statistics were populated
        assert component.stats['files_discovered'] == 1
        assert component.stats['files_processed'] == 1
        assert component.stats['files_failed'] == 0
        assert component.stats['records_ingested'] == len(result)

    def test_real_data_incremental_processing(self, real_config):
        """Test incremental processing with real data directory."""
        component = ParquetIngestionComponent(real_config)

        # First run - should process all files
        result1 = component.execute(force_full_reload=True)
        files_processed_1 = component.stats['files_processed']

        # Reset stats for second run
        component.stats = {
            "files_discovered": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "records_ingested": 0,
            "records_skipped": 0
        }

        # Second run - incremental mode (should process fewer/no files)
        result2 = component.execute(force_full_reload=False)
        files_processed_2 = component.stats['files_processed']

        # In incremental mode, should process fewer files (or same if checkpoint was empty)
        assert files_processed_2 <= files_processed_1

        print(f"\\nIncremental processing test:")
        print(f"Full reload: {files_processed_1} files")
        print(f"Incremental: {files_processed_2} files")

    def test_real_config_validation(self, real_config):
        """Test that the real configuration is valid and complete."""
        # Verify all required paths exist or can be created
        raw_path = Path(real_config.paths.data_raw)
        assert raw_path.exists(), f"Raw data path should exist: {raw_path}"

        processed_path = Path(real_config.paths.data_processed)
        assert processed_path.parent.exists(), f"Processed data parent should exist: {processed_path.parent}"

        # Verify schema configuration
        assert len(real_config.data_schema.expected_columns) > 0
        assert len(real_config.data_schema.types) > 0

        # Verify ranges are configured
        assert len(real_config.ranges) > 0

        # Verify ingestion settings
        assert real_config.ingestion.checkpoint_file is not None

    def test_real_data_edge_cases(self, real_config):
        """Test edge cases with real data directory."""
        component = ParquetIngestionComponent(real_config)

        # Test with non-existent specific file
        non_existent_file = Path(real_config.paths.data_raw) / "non_existent_file.parquet"
        result = component.execute(data_path=non_existent_file)

        # Should handle gracefully and return empty DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert component.stats['files_failed'] == 1


@pytest.mark.slow
class TestIngestionPerformance:
    """Performance tests for ingestion component (marked as slow)."""

    def test_large_file_processing_performance(self):
        """Test performance with available data files."""
        from src.config import PipelineConfig
        from pathlib import Path

        config_path = Path("config/default.yaml")
        if not config_path.exists():
            pytest.skip("Real config file not found")

        real_config = PipelineConfig.from_yaml(config_path)
        component = ParquetIngestionComponent(real_config)

        import time
        start_time = time.time()

        result = component.execute(force_full_reload=True)

        execution_time = time.time() - start_time

        print(f"\\nPerformance metrics:")
        print(f"Records processed: {len(result)}")
        print(f"Execution time: {execution_time:.2f} seconds")
        print(f"Records per second: {len(result) / execution_time:.0f}")

        # Performance assertions (adjust based on your requirements)
        assert execution_time < 30, "Processing should complete within 30 seconds"
        if len(result) > 0:
            assert len(result) / execution_time > 1, "Should process at least 1 record per second"