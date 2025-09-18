"""
Data transformation component for agricultural sensor data pipeline.

Handles data cleaning, derived field calculation, sensor calibration,
and enrichment of raw sensor readings.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from scipy import stats

from src.components.base import TransformationComponent
from src.config import PipelineConfig
from src.utils import get_logger, TransformationError


class AgricultureTransformationComponent(TransformationComponent):
    """Concrete implementation of transformation component for agricultural sensor data."""

    def __init__(self, config: PipelineConfig):
        """
        Initialize transformation component.

        Args:
            config: Pipeline configuration
        """
        super().__init__(config)
        self.logger = get_logger(__name__)

        # Transformation statistics
        self.stats = {
            "input_records": 0,
            "output_records": 0,
            "duplicates_removed": 0,
            "outliers_detected": 0,
            "missing_values_handled": 0,
            "records_calibrated": 0
        }

    def execute(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Execute data transformation pipeline.

        Args:
            raw_data: Raw sensor data from ingestion

        Returns:
            Transformed and enriched sensor data

        Raises:
            TransformationError: If transformation fails
        """
        try:
            self.logger.info("Starting data transformation")
            self.stats["input_records"] = len(raw_data)

            if raw_data.empty:
                self.logger.warning("No data to transform")
                return raw_data

            # Step 1: Data Cleaning
            self.logger.info("Step 1: Data Cleaning")
            cleaned_data = self._clean_data(raw_data)

            # Step 2: Sensor Calibration
            self.logger.info("Step 2: Sensor Calibration")
            calibrated_data = self._apply_calibration(cleaned_data)

            # Step 3: Timezone Conversion
            self.logger.info("Step 3: Timezone Conversion")
            timezone_data = self._convert_timezone(calibrated_data)

            # Step 4: Derived Fields
            self.logger.info("Step 4: Derived Fields Calculation")
            enriched_data = self._calculate_derived_fields(timezone_data)

            # Step 5: Anomaly Detection
            self.logger.info("Step 5: Anomaly Detection")
            final_data = self._detect_anomalies(enriched_data)

            self.stats["output_records"] = len(final_data)
            self._log_transformation_summary()

            self.logger.info(f"Transformation completed: {len(final_data)} records")
            return final_data

        except Exception as e:
            self.logger.error(f"Transformation failed: {str(e)}")
            raise TransformationError(f"Data transformation failed: {str(e)}") from e

    def _clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Clean raw data by removing duplicates and handling missing values.

        Args:
            data: Raw sensor data

        Returns:
            Cleaned data
        """
        # Create a copy to avoid modifying original data
        cleaned_data = data.copy()

        # Remove exact duplicates
        initial_count = len(cleaned_data)
        cleaned_data = cleaned_data.drop_duplicates()
        duplicates_removed = initial_count - len(cleaned_data)
        self.stats["duplicates_removed"] = duplicates_removed

        if duplicates_removed > 0:
            self.logger.info(f"   Removed {duplicates_removed} duplicate records")

        # Handle missing values
        missing_before = cleaned_data.isnull().sum().sum()
        if missing_before > 0:
            self.logger.info(f"   Found {missing_before} missing values")

            # Strategy: Drop rows with missing critical fields
            critical_fields = ['sensor_id', 'timestamp', 'reading_type', 'value']
            cleaned_data = cleaned_data.dropna(subset=critical_fields)

            # Fill missing battery_level with median per sensor
            # Avoid computing median on all-NaN groups (which triggers NumPy warnings)
            if 'battery_level' in cleaned_data.columns:
                bl = cleaned_data['battery_level']
                global_median = bl.median() if bl.notna().any() else np.nan

                def _fill_sensor_battery(x: pd.Series) -> pd.Series:
                    # If the group has any valid values, use its median; otherwise fallback to global median
                    if x.notna().any():
                        return x.fillna(x.median())
                    else:
                        return x.fillna(global_median)

                cleaned_data['battery_level'] = (
                    cleaned_data.groupby('sensor_id')['battery_level']
                    .transform(_fill_sensor_battery)
                )

            missing_after = cleaned_data.isnull().sum().sum()
            self.stats["missing_values_handled"] = missing_before - missing_after

            if missing_after > 0:
                self.logger.warning(f"   {missing_after} missing values remain after cleaning")

        return cleaned_data

    def _apply_calibration(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Apply sensor calibration using multiplier and offset from configuration.

        Args:
            data: Cleaned sensor data

        Returns:
            Calibrated data
        """
        calibrated_data = data.copy()
        calibrated_count = 0

        # Apply calibration per reading type
        for reading_type in calibrated_data['reading_type'].unique():
            calibration = self.config.calibration.get(reading_type)

            if calibration:
                mask = calibrated_data['reading_type'] == reading_type
                original_values = calibrated_data.loc[mask, 'value'].copy()

                # Apply calibration: value = raw_value * multiplier + offset
                calibrated_data.loc[mask, 'value'] = (
                    original_values * calibration.multiplier + calibration.offset
                )

                calibrated_count += mask.sum()

                if calibration.multiplier != 1.0 or calibration.offset != 0.0:
                    self.logger.info(
                        f"   Applied calibration to {reading_type}: "
                        f"multiplier={calibration.multiplier}, offset={calibration.offset}"
                    )

        self.stats["records_calibrated"] = calibrated_count
        return calibrated_data

    def _convert_timezone(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Convert timestamps to target timezone.

        Args:
            data: Calibrated sensor data

        Returns:
            Data with timezone-converted timestamps
        """
        timezone_data = data.copy()

        # Parse target timezone from config (e.g., "UTC+05:30")
        target_tz_str = self.config.project.timezone

        # Convert UTC+05:30 format to timedelta
        if target_tz_str.startswith("UTC"):
            offset_str = target_tz_str[3:]  # Remove "UTC" prefix
            if offset_str:
                sign = 1 if offset_str[0] == '+' else -1
                time_part = offset_str[1:]  # Remove +/- sign
                hours, minutes = map(int, time_part.split(':'))
                offset = timedelta(hours=sign*hours, minutes=sign*minutes)
                target_tz = timezone(offset)
            else:
                target_tz = timezone.utc
        else:
            target_tz = timezone.utc

        # Ensure timestamp column is datetime
        if not pd.api.types.is_datetime64_any_dtype(timezone_data['timestamp']):
            timezone_data['timestamp'] = pd.to_datetime(timezone_data['timestamp'])

        # Convert to target timezone
        if timezone_data['timestamp'].dt.tz is None:
            # Assume UTC if no timezone info
            timezone_data['timestamp'] = timezone_data['timestamp'].dt.tz_localize('UTC')

        timezone_data['timestamp'] = timezone_data['timestamp'].dt.tz_convert(target_tz)

        self.logger.info(f"   Converted timestamps to {target_tz_str}")
        return timezone_data

    def _calculate_derived_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived fields like daily averages and rolling averages.

        Args:
            data: Timezone-converted sensor data

        Returns:
            Data with derived fields added
        """
        enriched_data = data.copy()

        # Add date column for daily aggregations
        enriched_data['date'] = enriched_data['timestamp'].dt.date

        # Calculate daily average per sensor and reading type
        daily_avg = enriched_data.groupby(['sensor_id', 'reading_type', 'date'])['value'].mean().reset_index()
        daily_avg.columns = ['sensor_id', 'reading_type', 'date', 'daily_avg_value']

        # Merge back to main data
        enriched_data = enriched_data.merge(
            daily_avg,
            on=['sensor_id', 'reading_type', 'date'],
            how='left'
        )

        # Calculate rolling average (convert days to observations)
        rolling_window_days = self.config.transformation.rolling_window_days

        # Sort by sensor, reading_type, and timestamp for rolling calculation
        enriched_data = enriched_data.sort_values(['sensor_id', 'reading_type', 'timestamp'])

        # For simplicity, use a fixed window of observations (could be enhanced to use time-based windows)
        # Assuming roughly hourly readings, 7 days ≈ 7*24 = 168 observations
        rolling_window_obs = rolling_window_days * 24  # Approximate observations per day

        # Calculate rolling average with observation-based window
        enriched_data['rolling_avg_value'] = enriched_data.groupby(['sensor_id', 'reading_type'])['value'].transform(
            lambda x: x.rolling(window=min(rolling_window_obs, len(x)), min_periods=1).mean()
        )

        self.logger.info(f"   Added daily averages and {rolling_window_days}-day rolling averages")
        return enriched_data

    def _detect_anomalies(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Detect anomalies using Z-score and value range checks.

        Args:
            data: Data with derived fields

        Returns:
            Data with anomaly flags added
        """
        anomaly_data = data.copy()
        anomaly_data['anomalous_reading'] = False

        # Z-score based outlier detection per reading type
        z_threshold = self.config.transformation.z_score_threshold

        for reading_type in anomaly_data['reading_type'].unique():
            mask = anomaly_data['reading_type'] == reading_type
            values = anomaly_data.loc[mask, 'value']

            if len(values) > 1:  # Need at least 2 values for z-score
                z_scores = np.abs(stats.zscore(values, nan_policy='omit'))
                outlier_mask = z_scores > z_threshold

                # Mark as anomalous
                anomaly_data.loc[mask, 'anomalous_reading'] |= outlier_mask
            elif len(values) == 1:
                # Log when we can't compute z-score for single values
                self.logger.debug(f"   Cannot compute Z-score for {reading_type}: only 1 value")

        # Range-based anomaly detection for reading_type values
        for reading_type, value_range in self.config.ranges.items():
            if reading_type == 'battery_level':
                continue  # Handle battery separately below

            mask = anomaly_data['reading_type'] == reading_type
            values = anomaly_data.loc[mask, 'value']

            if len(values) > 0:
                # Check if values are outside expected range
                range_outliers = (values < value_range.min) | (values > value_range.max)
                anomaly_data.loc[mask, 'anomalous_reading'] |= range_outliers

        # Battery level validation (separate column, not reading_type)
        battery_range = self.config.ranges.get('battery_level')
        if battery_range and 'battery_level' in anomaly_data.columns:
            battery_outliers = ((anomaly_data['battery_level'] < battery_range.min) |
                              (anomaly_data['battery_level'] > battery_range.max))

            # For battery, we could create a separate flag or log warnings
            battery_anomaly_count = battery_outliers.sum()
            if battery_anomaly_count > 0:
                self.logger.warning(f"   Found {battery_anomaly_count} battery level anomalies (not flagged as reading anomalies)")

        # Fix: Count unique anomalous readings (avoid double-counting)
        outliers_count = int(anomaly_data['anomalous_reading'].sum())
        self.stats["outliers_detected"] = outliers_count

        if outliers_count > 0:
            self.logger.info(f"   Detected {outliers_count} anomalous readings")

        # Apply outlier handling based on configuration
        outlier_handling = self.config.transformation.outlier_handling.lower()

        if outlier_handling == "remove":
            initial_count = len(anomaly_data)
            anomaly_data = anomaly_data[~anomaly_data['anomalous_reading']]
            removed_count = initial_count - len(anomaly_data)

            if removed_count > 0:
                self.logger.info(f"   Removed {removed_count} anomalous readings (outlier_handling='remove')")

        elif outlier_handling == "flag":
            # Default behavior: keep anomalous readings but mark them
            pass
        else:
            self.logger.warning(f"   Unknown outlier_handling mode: '{outlier_handling}', defaulting to 'flag'")

        return anomaly_data

    def _log_transformation_summary(self) -> None:
        """Log comprehensive transformation statistics."""
        self.logger.info("=== Transformation Summary ===")
        for key, value in self.stats.items():
            self.logger.info(f"{key.replace('_', ' ').title()}: {value}")

        # Calculate transformation efficiency
        if self.stats["input_records"] > 0:
            efficiency = (self.stats["output_records"] / self.stats["input_records"]) * 100
            self.logger.info(f"Data Retention Rate: {efficiency:.1f}%")