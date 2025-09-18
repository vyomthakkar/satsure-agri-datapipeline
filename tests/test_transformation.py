"""
Comprehensive tests for the transformation component.

Tests cover data cleaning, calibration, derived fields calculation,
anomaly detection, and transformation statistics.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from src.components.transformation import AgricultureTransformationComponent
from src.utils.exceptions import TransformationError


class TestAgricultureTransformationComponent:
    """Test suite for AgricultureTransformationComponent."""

    def test_init(self, sample_config):
        """Test component initialization."""
        component = AgricultureTransformationComponent(sample_config)

        assert component.config == sample_config
        assert component.logger is not None
        assert component.stats["input_records"] == 0
        assert component.stats["output_records"] == 0

    def test_execute_with_valid_data(self, sample_config, sample_sensor_data):
        """Test successful transformation execution."""
        component = AgricultureTransformationComponent(sample_config)

        result = component.execute(sample_sensor_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_sensor_data)

        # Check new columns were added
        expected_new_columns = ['date', 'daily_avg_value', 'rolling_avg_value', 'anomalous_reading']
        for col in expected_new_columns:
            assert col in result.columns

        # Check statistics
        assert component.stats["input_records"] == len(sample_sensor_data)
        assert component.stats["output_records"] == len(result)

    def test_execute_with_empty_data(self, sample_config):
        """Test transformation with empty DataFrame."""
        component = AgricultureTransformationComponent(sample_config)
        empty_data = pd.DataFrame()

        result = component.execute(empty_data)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert component.stats["input_records"] == 0

    def test_data_cleaning_removes_duplicates(self, sample_config):
        """Test that duplicate records are removed."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data with duplicates
        data = pd.DataFrame({
            'sensor_id': ['sensor_1', 'sensor_1', 'sensor_2'],
            'timestamp': [datetime(2023, 6, 1, 10, 0), datetime(2023, 6, 1, 10, 0), datetime(2023, 6, 1, 10, 15)],
            'reading_type': ['temperature', 'temperature', 'humidity'],
            'value': [25.0, 25.0, 60.0],
            'battery_level': [90.0, 90.0, 85.0]
        })

        result = component.execute(data)

        assert len(result) == 2  # One duplicate should be removed
        assert component.stats["duplicates_removed"] == 1

    def test_data_cleaning_handles_missing_values(self, sample_config):
        """Test missing value handling."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data with missing values
        data = pd.DataFrame({
            'sensor_id': ['sensor_1', 'sensor_2', 'sensor_3'],
            'timestamp': [datetime(2023, 6, 1, 10, 0), datetime(2023, 6, 1, 10, 15), datetime(2023, 6, 1, 10, 30)],
            'reading_type': ['temperature', 'humidity', 'temperature'],
            'value': [25.0, 60.0, np.nan],  # Missing value
            'battery_level': [90.0, np.nan, 85.0]  # Missing battery
        })

        result = component.execute(data)

        # Should drop row with missing critical value
        assert len(result) == 2
        assert component.stats["missing_values_handled"] > 0

    def test_sensor_calibration(self, sample_config):
        """Test sensor calibration application."""
        component = AgricultureTransformationComponent(sample_config)

        # Create test data
        data = pd.DataFrame({
            'sensor_id': ['sensor_1', 'sensor_2'],
            'timestamp': [datetime(2023, 6, 1, 10, 0), datetime(2023, 6, 1, 10, 15)],
            'reading_type': ['temperature', 'humidity'],
            'value': [20.0, 50.0],
            'battery_level': [90.0, 85.0]
        })

        result = component.execute(data)

        # Check calibration was applied (multiplier=1.0, offset=0.0 in sample config)
        assert component.stats["records_calibrated"] == 2

        # Values should be unchanged with default calibration (1.0 * value + 0.0)
        temperature_row = result[result['reading_type'] == 'temperature'].iloc[0]
        assert temperature_row['value'] == 20.0

    def test_timezone_conversion(self, sample_config):
        """Test timezone conversion to target timezone."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data with UTC timezone
        utc_time = datetime(2023, 6, 1, 10, 0, tzinfo=timezone.utc)
        data = pd.DataFrame({
            'sensor_id': ['sensor_1'],
            'timestamp': [utc_time],
            'reading_type': ['temperature'],
            'value': [25.0],
            'battery_level': [90.0]
        })

        result = component.execute(data)

        # Check timezone was converted (UTC+05:30)
        result_time = result.iloc[0]['timestamp']
        assert result_time.tzinfo is not None
        # Should be 5.5 hours ahead of UTC
        expected_offset = timedelta(hours=5, minutes=30)
        assert result_time.utcoffset() == expected_offset

    def test_derived_fields_calculation(self, sample_config):
        """Test daily average and rolling average calculations."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data spanning multiple timestamps for same sensor/reading_type
        data = pd.DataFrame({
            'sensor_id': ['sensor_1', 'sensor_1', 'sensor_1'],
            'timestamp': [
                datetime(2023, 6, 1, 10, 0),
                datetime(2023, 6, 1, 11, 0),
                datetime(2023, 6, 1, 12, 0)
            ],
            'reading_type': ['temperature', 'temperature', 'temperature'],
            'value': [20.0, 25.0, 30.0],
            'battery_level': [90.0, 85.0, 80.0]
        })

        result = component.execute(data)

        # Check derived fields exist
        assert 'daily_avg_value' in result.columns
        assert 'rolling_avg_value' in result.columns

        # Daily average should be 25.0 for all records (same day)
        assert all(result['daily_avg_value'] == 25.0)

        # Rolling averages should increase progressively
        rolling_values = result['rolling_avg_value'].tolist()
        assert rolling_values[0] == 20.0  # First value
        assert rolling_values[1] == 22.5  # (20+25)/2
        assert rolling_values[2] == 25.0  # (20+25+30)/3

    def test_anomaly_detection_with_outliers(self, sample_config):
        """Test anomaly detection flags outliers correctly."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data with normal values around ~25 and two clear outliers (-50, 100)
        data = pd.DataFrame({
            'sensor_id': ['sensor_1'] * 8,
            'timestamp': [datetime(2023, 6, 1, 10, i) for i in range(8)],
            'reading_type': ['temperature'] * 8,
            'value': [25.0, 24.5, 25.5, 26.0, -50.0, 100.0, 24.8, 25.2],
            'battery_level': [90.0] * 8
        })

        result = component.execute(data)

        # Outliers should be flagged
        outliers = result[result['anomalous_reading'] == True]
        normals = result[result['anomalous_reading'] == False]

        assert set(outliers['value']) == {-50.0, 100.0}
        assert set(normals['value']) == {25.0, 24.5, 25.5, 26.0, 24.8, 25.2}

        # Stats should reflect the number of detected outliers (no double-counting)
        assert component.stats["outliers_detected"] == 2

    def test_anomaly_detection_z_score_threshold(self, sample_config):
        """Test Z-score based anomaly detection."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data with clear outliers (Z-score > 3)
        # Normal values around 25, one extreme outlier
        data = pd.DataFrame({
            'sensor_id': ['sensor_1'] * 6,
            'timestamp': [datetime(2023, 6, 1, 10, i) for i in range(6)],
            'reading_type': ['temperature'] * 6,
            'value': [25.0, 24.0, 26.0, 25.5, 24.5, 100.0],  # 100.0 is clear outlier
            'battery_level': [90.0] * 6
        })

        result = component.execute(data)

        # Check that outlier was detected
        outliers = result[result['anomalous_reading'] == True]
        assert len(outliers) > 0
        assert component.stats["outliers_detected"] > 0

        # The extreme value should be flagged
        extreme_row = result[result['value'] == 100.0].iloc[0]
        assert extreme_row['anomalous_reading'] == True

    def test_anomaly_detection_range_based(self, sample_config):
        """Test range-based anomaly detection."""
        component = AgricultureTransformationComponent(sample_config)

        # Create temperature data outside config range (-10 to 60)
        data = pd.DataFrame({
            'sensor_id': ['sensor_1', 'sensor_2'],
            'timestamp': [datetime(2023, 6, 1, 10, 0), datetime(2023, 6, 1, 10, 15)],
            'reading_type': ['temperature', 'temperature'],
            'value': [-20.0, 80.0],  # Both outside range
            'battery_level': [90.0, 85.0]
        })

        result = component.execute(data)

        # Both should be flagged as anomalous
        assert all(result['anomalous_reading'] == True)
        assert component.stats["outliers_detected"] >= 2

    def test_transformation_statistics_tracking(self, sample_config):
        """Test that transformation statistics are tracked correctly."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data with various conditions
        data = pd.DataFrame({
            'sensor_id': ['sensor_1', 'sensor_1', 'sensor_2', 'sensor_2'],
            'timestamp': [
                datetime(2023, 6, 1, 10, 0),
                datetime(2023, 6, 1, 10, 0),  # Duplicate
                datetime(2023, 6, 1, 10, 15),
                datetime(2023, 6, 1, 10, 30)
            ],
            'reading_type': ['temperature', 'temperature', 'humidity', 'temperature'],
            'value': [25.0, 25.0, 60.0, -50.0],  # Last value is outlier
            'battery_level': [90.0, 90.0, 85.0, 80.0]
        })

        result = component.execute(data)

        # Check statistics
        assert component.stats["input_records"] == 4
        assert component.stats["duplicates_removed"] == 1
        assert component.stats["output_records"] == 3
        assert component.stats["outliers_detected"] > 0

    def test_outlier_handling_remove_mode(self, sample_config):
        """Test outlier handling in 'remove' mode."""
        # Modify config to remove outliers
        sample_config.transformation.outlier_handling = "remove"
        component = AgricultureTransformationComponent(sample_config)

        # Create data with clear outlier
        data = pd.DataFrame({
            'sensor_id': ['sensor_1'] * 4,
            'timestamp': [datetime(2023, 6, 1, 10, i) for i in range(4)],
            'reading_type': ['temperature'] * 4,
            'value': [25.0, 24.0, 26.0, -50.0],  # -50.0 is outlier (outside range)
            'battery_level': [90.0] * 4
        })

        result = component.execute(data)

        # Should remove outlier row
        assert len(result) == 3  # One record removed
        assert -50.0 not in result['value'].values
        assert component.stats["outliers_detected"] == 1

    def test_outlier_handling_flag_mode(self, sample_config):
        """Test outlier handling in 'flag' mode (default)."""
        # Ensure flag mode
        sample_config.transformation.outlier_handling = "flag"
        component = AgricultureTransformationComponent(sample_config)

        # Create data with clear outlier
        data = pd.DataFrame({
            'sensor_id': ['sensor_1'] * 4,
            'timestamp': [datetime(2023, 6, 1, 10, i) for i in range(4)],
            'reading_type': ['temperature'] * 4,
            'value': [25.0, 24.0, 26.0, -50.0],  # -50.0 is outlier
            'battery_level': [90.0] * 4
        })

        result = component.execute(data)

        # Should keep all records but flag outlier
        assert len(result) == 4  # All records kept
        assert -50.0 in result['value'].values
        outliers = result[result['anomalous_reading'] == True]
        assert len(outliers) == 1
        assert outliers.iloc[0]['value'] == -50.0

    def test_battery_level_validation(self, sample_config):
        """Test separate battery level validation."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data with battery outliers
        data = pd.DataFrame({
            'sensor_id': ['sensor_1', 'sensor_2'],
            'timestamp': [datetime(2023, 6, 1, 10, 0), datetime(2023, 6, 1, 10, 15)],
            'reading_type': ['temperature', 'humidity'],
            'value': [25.0, 60.0],
            'battery_level': [150.0, -10.0]  # Both outside 0-100 range
        })

        with patch.object(component.logger, 'warning') as mock_warning:
            result = component.execute(data)

            # Should log battery anomalies but not flag as reading anomalies
            mock_warning.assert_called()
            warning_calls = [call.args[0] for call in mock_warning.call_args_list]
            battery_warnings = [call for call in warning_calls if "battery level anomalies" in call]
            assert len(battery_warnings) > 0

        # Reading anomalies should be False (battery issues don't affect reading flags)
        assert all(result['anomalous_reading'] == False)

    def test_statistics_no_double_counting(self, sample_config):
        """Test that outlier statistics don't double-count records flagged by multiple methods."""
        component = AgricultureTransformationComponent(sample_config)

        # Create data where same record would be flagged by both Z-score and range
        data = pd.DataFrame({
            'sensor_id': ['sensor_1'] * 5,
            'timestamp': [datetime(2023, 6, 1, 10, i) for i in range(5)],
            'reading_type': ['temperature'] * 5,
            'value': [25.0, 24.0, 26.0, 25.5, 200.0],  # 200.0 fails both Z-score and range
            'battery_level': [90.0] * 5
        })

        result = component.execute(data)

        # Should count 200.0 outlier only once, not twice
        outliers = result[result['anomalous_reading'] == True]
        assert len(outliers) == 1  # Only one outlier
        assert component.stats["outliers_detected"] == 1  # Count should match actual flagged records

    def test_error_handling(self, sample_config):
        """Test error handling for invalid data."""
        component = AgricultureTransformationComponent(sample_config)

        # Test with invalid data structure
        invalid_data = "not a dataframe"

        with pytest.raises(TransformationError):
            component.execute(invalid_data)

    def test_log_transformation_summary(self, sample_config):
        """Test transformation summary logging."""
        component = AgricultureTransformationComponent(sample_config)

        # Set some stats
        component.stats.update({
            "input_records": 100,
            "output_records": 95,
            "duplicates_removed": 5,
            "outliers_detected": 3
        })

        with patch.object(component.logger, 'info') as mock_info:
            component._log_transformation_summary()

            # Should log summary and retention rate
            assert mock_info.call_count >= 6  # Stats + retention rate

            # Check retention rate calculation
            calls = [call.args[0] for call in mock_info.call_args_list]
            retention_call = [call for call in calls if "Data Retention Rate" in call]
            assert len(retention_call) == 1
            assert "95.0%" in retention_call[0]