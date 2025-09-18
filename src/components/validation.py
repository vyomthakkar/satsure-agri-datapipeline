"""
Data quality validation component for agricultural sensor data pipeline.

Provides comprehensive validation using DuckDB queries including type validation,
range checks, gap detection, and data quality profiling.
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Any

from src.components.base import ValidationComponent
from src.config import PipelineConfig
from src.models import ValidationResult
from src.utils import get_logger, ValidationError


class AgricultureValidationComponent(ValidationComponent):
    """Concrete implementation of validation component for agricultural sensor data."""

    def __init__(self, config: PipelineConfig):
        """
        Initialize validation component.

        Args:
            config: Pipeline configuration
        """
        super().__init__(config)
        self.logger = get_logger(__name__)
        self.duckdb_conn = duckdb.connect(':memory:')

        # Validation statistics
        self.stats = {
            "total_records": 0,
            "type_violations": 0,
            "range_violations": 0,
            "missing_values": 0,
            "anomalous_readings": 0,
            "time_gaps_detected": 0,
            "sensors_validated": 0
        }

        # Quality thresholds from config
        self.quality_thresholds = {
            "max_missing_percentage": self.config.validation.max_missing_percentage,
            "max_anomaly_percentage": self.config.validation.max_anomaly_percentage,
            "max_gap_hours": self.config.validation.max_gap_hours
        }

        # Expected reading frequency from config
        self.expected_frequency_hours = self.config.validation.expected_frequency_hours

    def execute(self, transformed_data: pd.DataFrame) -> ValidationResult:
        """
        Execute comprehensive data quality validation.

        Args:
            transformed_data: Transformed data from transformation component

        Returns:
            ValidationResult with quality metrics and issues
        """
        try:
            self.logger.info("Starting data quality validation")
            self.stats["total_records"] = len(transformed_data)

            if transformed_data.empty:
                self.logger.warning("No data to validate")
                return ValidationResult(
                    passed=False,
                    total_records=0,
                    issues_found=["No data provided for validation"],
                    quality_metrics={}
                )

            # Load data into DuckDB for validation queries
            self.duckdb_conn.register('sensor_data', transformed_data)

            issues_found = []
            quality_metrics = {}

            # Step 1: Type Validation
            self.logger.info("Step 1: Type Validation")
            type_issues = self._validate_types()
            issues_found.extend(type_issues)

            # Step 2: Value Range Validation
            self.logger.info("Step 2: Value Range Validation")
            range_issues = self._validate_value_ranges()
            issues_found.extend(range_issues)

            # Step 3: Gap Detection
            self.logger.info("Step 3: Gap Detection")
            gap_issues, gap_metrics = self._detect_time_gaps()
            issues_found.extend(gap_issues)
            quality_metrics.update(gap_metrics)

            # Step 4: Data Profiling
            self.logger.info("Step 4: Data Profiling")
            profile_metrics = self._profile_data_quality()
            quality_metrics.update(profile_metrics)

            # Step 5: Overall Quality Assessment
            overall_passed = self._assess_overall_quality(quality_metrics)

            # Step 6: Generate Quality Report
            self._generate_quality_report(quality_metrics, issues_found)

            # Log validation summary
            self._log_validation_summary(issues_found, quality_metrics)

            return ValidationResult(
                passed=overall_passed,
                total_records=self.stats["total_records"],
                issues_found=issues_found,
                quality_metrics=quality_metrics
            )

        except Exception as e:
            self.logger.error(f"Validation failed: {str(e)}")
            raise ValidationError(f"Data validation failed: {str(e)}") from e

    def _validate_types(self) -> List[str]:
        """
        Validate data types using DuckDB queries.

        Returns:
            List of type validation issues
        """
        issues = []

        try:
            # Check for non-numeric values in value column
            non_numeric_values = self.duckdb_conn.execute("""
                SELECT COUNT(*) as count
                FROM sensor_data
                WHERE NOT (value IS NULL OR TRY_CAST(value AS DOUBLE) IS NOT NULL)
            """).fetchone()[0]

            if non_numeric_values > 0:
                issues.append(f"Found {non_numeric_values} non-numeric values in 'value' column")
                self.stats["type_violations"] += non_numeric_values

            # Check for non-numeric battery levels
            non_numeric_battery = self.duckdb_conn.execute("""
                SELECT COUNT(*) as count
                FROM sensor_data
                WHERE NOT (battery_level IS NULL OR TRY_CAST(battery_level AS DOUBLE) IS NOT NULL)
            """).fetchone()[0]

            if non_numeric_battery > 0:
                issues.append(f"Found {non_numeric_battery} non-numeric values in 'battery_level' column")
                self.stats["type_violations"] += non_numeric_battery

            # Check timestamp format (should already be datetime from transformation)
            invalid_timestamps = self.duckdb_conn.execute("""
                SELECT COUNT(*) as count
                FROM sensor_data
                WHERE timestamp IS NULL
            """).fetchone()[0]

            if invalid_timestamps > 0:
                issues.append(f"Found {invalid_timestamps} invalid/null timestamps")
                self.stats["type_violations"] += invalid_timestamps

            self.logger.info(f"   Type validation completed: {len(issues)} issues found")

        except Exception as e:
            issues.append(f"Type validation failed: {str(e)}")

        return issues

    def _validate_value_ranges(self) -> List[str]:
        """
        Validate values are within expected ranges per reading_type.

        Returns:
            List of range validation issues
        """
        issues = []

        try:
            for reading_type, value_range in self.config.ranges.items():
                if reading_type == 'battery_level':
                    # Validate battery levels separately
                    out_of_range = self.duckdb_conn.execute(f"""
                        SELECT COUNT(*) as count
                        FROM sensor_data
                        WHERE battery_level < {value_range.min} OR battery_level > {value_range.max}
                    """).fetchone()[0]

                    if out_of_range > 0:
                        issues.append(f"Found {out_of_range} battery_level values outside range [{value_range.min}, {value_range.max}]")
                        self.stats["range_violations"] += out_of_range
                else:
                    # Validate reading_type values
                    out_of_range = self.duckdb_conn.execute(f"""
                        SELECT COUNT(*) as count
                        FROM sensor_data
                        WHERE reading_type = '{reading_type}'
                        AND (value < {value_range.min} OR value > {value_range.max})
                    """).fetchone()[0]

                    if out_of_range > 0:
                        issues.append(f"Found {out_of_range} {reading_type} values outside range [{value_range.min}, {value_range.max}]")
                        self.stats["range_violations"] += out_of_range

            self.logger.info(f"   Range validation completed: {self.stats['range_violations']} violations found")

        except Exception as e:
            issues.append(f"Range validation failed: {str(e)}")

        return issues

    def _detect_time_gaps(self) -> Tuple[List[str], Dict[str, Any]]:
        """
        Detect gaps in hourly data using DuckDB generate_series.

        Returns:
            Tuple of (issues list, gap metrics)
        """
        issues = []
        gap_metrics = {}

        try:
            # Get overall time range
            time_range = self.duckdb_conn.execute("""
                SELECT
                    MIN(timestamp) as min_time,
                    MAX(timestamp) as max_time,
                    COUNT(DISTINCT sensor_id) as sensor_count
                FROM sensor_data
            """).fetchone()

            min_time, max_time, sensor_count = time_range
            self.stats["sensors_validated"] = sensor_count

            gap_metrics.update({
                "time_range_start": str(min_time),
                "time_range_end": str(max_time),
                "sensors_analyzed": sensor_count
            })

            # Generate expected timestamps based on configured frequency
            frequency_interval = f"{self.expected_frequency_hours} hour"
            self.duckdb_conn.execute(f"""
                CREATE OR REPLACE TABLE expected_times AS
                SELECT timestamp_val as expected_timestamp
                FROM generate_series(
                    TIMESTAMP '{min_time}',
                    TIMESTAMP '{max_time}',
                    INTERVAL '{frequency_interval}'
                ) AS t(timestamp_val)
            """)

            # Find gaps per sensor and reading_type
            gap_analysis = self.duckdb_conn.execute("""
                WITH sensor_reading_combinations AS (
                    SELECT DISTINCT sensor_id, reading_type
                    FROM sensor_data
                ),
                expected_readings AS (
                    SELECT
                        src.sensor_id,
                        src.reading_type,
                        et.expected_timestamp
                    FROM sensor_reading_combinations src
                    CROSS JOIN expected_times et
                ),
                missing_readings AS (
                    SELECT
                        er.sensor_id,
                        er.reading_type,
                        er.expected_timestamp
                    FROM expected_readings er
                    LEFT JOIN sensor_data sd ON (
                        er.sensor_id = sd.sensor_id
                        AND er.reading_type = sd.reading_type
                        AND DATE_TRUNC('hour', er.expected_timestamp) = DATE_TRUNC('hour', sd.timestamp)
                    )
                    WHERE sd.sensor_id IS NULL
                )
                SELECT
                    sensor_id,
                    reading_type,
                    COUNT(*) as missing_hours,
                    MIN(expected_timestamp) as first_gap,
                    MAX(expected_timestamp) as last_gap
                FROM missing_readings
                GROUP BY sensor_id, reading_type
                ORDER BY missing_hours DESC
            """).fetchall()

            total_gaps = 0
            significant_gaps = []

            for sensor_id, reading_type, missing_hours, first_gap, last_gap in gap_analysis:
                total_gaps += missing_hours

                if missing_hours >= self.quality_thresholds["max_gap_hours"]:
                    significant_gaps.append({
                        "sensor_id": sensor_id,
                        "reading_type": reading_type,
                        "missing_hours": missing_hours,
                        "first_gap": str(first_gap),
                        "last_gap": str(last_gap)
                    })

                    issues.append(
                        f"Sensor {sensor_id} ({reading_type}): {missing_hours} hour gap from {first_gap} to {last_gap}"
                    )

            self.stats["time_gaps_detected"] = total_gaps

            gap_metrics.update({
                "total_missing_hours": total_gaps,
                "significant_gaps": significant_gaps,
                "gap_threshold_hours": self.quality_thresholds["max_gap_hours"]
            })

            self.logger.info(f"   Gap detection completed: {total_gaps} total missing {frequency_interval} intervals, {len(significant_gaps)} significant gaps")

        except Exception as e:
            issues.append(f"Gap detection failed: {str(e)}")
            gap_metrics["gap_detection_error"] = str(e)

        return issues, gap_metrics

    def _profile_data_quality(self) -> Dict[str, Any]:
        """
        Profile data quality metrics using DuckDB.

        Returns:
            Dictionary of quality metrics
        """
        quality_metrics = {}

        try:
            # Missing values analysis
            missing_analysis = self.duckdb_conn.execute("""
                SELECT
                    reading_type,
                    COUNT(*) as total_records,
                    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as missing_values,
                    ROUND(100.0 * SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as missing_percentage
                FROM sensor_data
                GROUP BY reading_type
                ORDER BY missing_percentage DESC
            """).fetchall()

            missing_by_type = {}
            total_missing = 0

            for reading_type, total_records, missing_values, missing_percentage in missing_analysis:
                missing_by_type[reading_type] = {
                    "total_records": total_records,
                    "missing_values": missing_values,
                    "missing_percentage": missing_percentage
                }
                total_missing += missing_values

            self.stats["missing_values"] = total_missing
            quality_metrics["missing_values_by_type"] = missing_by_type

            # Anomalous readings analysis
            anomaly_analysis = self.duckdb_conn.execute("""
                SELECT
                    reading_type,
                    COUNT(*) as total_records,
                    SUM(CASE WHEN anomalous_reading = true THEN 1 ELSE 0 END) as anomalous_count,
                    ROUND(100.0 * SUM(CASE WHEN anomalous_reading = true THEN 1 ELSE 0 END) / COUNT(*), 2) as anomaly_percentage
                FROM sensor_data
                WHERE anomalous_reading IS NOT NULL
                GROUP BY reading_type
                ORDER BY anomaly_percentage DESC
            """).fetchall()

            anomalies_by_type = {}
            total_anomalies = 0

            for reading_type, total_records, anomalous_count, anomaly_percentage in anomaly_analysis:
                anomalies_by_type[reading_type] = {
                    "total_records": total_records,
                    "anomalous_count": anomalous_count,
                    "anomaly_percentage": anomaly_percentage
                }
                total_anomalies += anomalous_count

            self.stats["anomalous_readings"] = total_anomalies
            quality_metrics["anomalies_by_type"] = anomalies_by_type

            # Sensor coverage analysis
            sensor_coverage = self.duckdb_conn.execute("""
                SELECT
                    sensor_id,
                    COUNT(DISTINCT reading_type) as reading_types,
                    COUNT(*) as total_readings,
                    MIN(timestamp) as first_reading,
                    MAX(timestamp) as last_reading,
                    ROUND(AVG(battery_level), 1) as avg_battery_level
                FROM sensor_data
                GROUP BY sensor_id
                ORDER BY total_readings DESC
            """).fetchall()

            coverage_by_sensor = {}
            for sensor_id, reading_types, total_readings, first_reading, last_reading, avg_battery in sensor_coverage:
                coverage_by_sensor[sensor_id] = {
                    "reading_types": reading_types,
                    "total_readings": total_readings,
                    "first_reading": str(first_reading),
                    "last_reading": str(last_reading),
                    "avg_battery_level": avg_battery
                }

            quality_metrics["sensor_coverage"] = coverage_by_sensor

            # Overall statistics
            overall_stats = self.duckdb_conn.execute("""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT sensor_id) as unique_sensors,
                    COUNT(DISTINCT reading_type) as unique_reading_types,
                    ROUND(AVG(value), 2) as avg_value,
                    ROUND(AVG(battery_level), 1) as avg_battery_level
                FROM sensor_data
            """).fetchone()

            quality_metrics["overall_statistics"] = {
                "total_records": overall_stats[0],
                "unique_sensors": overall_stats[1],
                "unique_reading_types": overall_stats[2],
                "average_value": overall_stats[3],
                "average_battery_level": overall_stats[4]
            }

            self.logger.info(f"   Data profiling completed: {total_missing} missing values, {total_anomalies} anomalies")

        except Exception as e:
            quality_metrics["profiling_error"] = str(e)
            self.logger.error(f"Data profiling failed: {str(e)}")

        return quality_metrics

    def _assess_overall_quality(self, quality_metrics: Dict[str, Any]) -> bool:
        """
        Assess overall data quality based on thresholds.

        Args:
            quality_metrics: Computed quality metrics

        Returns:
            True if data quality passes, False otherwise
        """
        passed = True

        try:
            # Check missing value thresholds
            missing_by_type = quality_metrics.get("missing_values_by_type", {})
            for reading_type, metrics in missing_by_type.items():
                if metrics["missing_percentage"] > self.quality_thresholds["max_missing_percentage"]:
                    passed = False
                    self.logger.warning(
                        f"Quality threshold exceeded: {reading_type} has {metrics['missing_percentage']}% missing values "
                        f"(threshold: {self.quality_thresholds['max_missing_percentage']}%)"
                    )

            # Check anomaly thresholds
            anomalies_by_type = quality_metrics.get("anomalies_by_type", {})
            for reading_type, metrics in anomalies_by_type.items():
                if metrics["anomaly_percentage"] > self.quality_thresholds["max_anomaly_percentage"]:
                    passed = False
                    self.logger.warning(
                        f"Quality threshold exceeded: {reading_type} has {metrics['anomaly_percentage']}% anomalies "
                        f"(threshold: {self.quality_thresholds['max_anomaly_percentage']}%)"
                    )

        except Exception as e:
            self.logger.error(f"Quality assessment failed: {str(e)}")
            passed = False

        return passed

    def _generate_quality_report(self, quality_metrics: Dict[str, Any], issues_found: List[str]) -> None:
        """
        Generate data_quality_report.csv as required by PROJECT.md.

        Args:
            quality_metrics: Quality metrics to include in report
            issues_found: List of issues found during validation
        """
        try:
            # Prepare report data
            report_data = []
            timestamp = datetime.now().isoformat()

            # Overall summary
            overall_stats = quality_metrics.get("overall_statistics", {})
            report_data.append({
                "category": "overall",
                "metric": "total_records",
                "value": overall_stats.get("total_records", 0),
                "threshold": None,
                "status": "pass",
                "details": f"Total records processed: {overall_stats.get('total_records', 0)}"
            })

            report_data.append({
                "category": "overall",
                "metric": "unique_sensors",
                "value": overall_stats.get("unique_sensors", 0),
                "threshold": None,
                "status": "info",
                "details": f"Unique sensors: {overall_stats.get('unique_sensors', 0)}"
            })

            # Missing values analysis
            missing_by_type = quality_metrics.get("missing_values_by_type", {})
            for reading_type, metrics in missing_by_type.items():
                status = "pass" if metrics["missing_percentage"] <= self.quality_thresholds["max_missing_percentage"] else "fail"
                report_data.append({
                    "category": "missing_values",
                    "metric": f"{reading_type}_missing_percentage",
                    "value": metrics["missing_percentage"],
                    "threshold": self.quality_thresholds["max_missing_percentage"],
                    "status": status,
                    "details": f"{metrics['missing_values']} out of {metrics['total_records']} records"
                })

            # Anomaly analysis
            anomalies_by_type = quality_metrics.get("anomalies_by_type", {})
            for reading_type, metrics in anomalies_by_type.items():
                status = "pass" if metrics["anomaly_percentage"] <= self.quality_thresholds["max_anomaly_percentage"] else "fail"
                report_data.append({
                    "category": "anomalies",
                    "metric": f"{reading_type}_anomaly_percentage",
                    "value": metrics["anomaly_percentage"],
                    "threshold": self.quality_thresholds["max_anomaly_percentage"],
                    "status": status,
                    "details": f"{metrics['anomalous_count']} out of {metrics['total_records']} records"
                })

            # Gap analysis
            gap_metrics = {k: v for k, v in quality_metrics.items() if "gap" in k or "missing_hours" in k}
            if "total_missing_hours" in quality_metrics:
                report_data.append({
                    "category": "time_gaps",
                    "metric": "total_missing_hours",
                    "value": quality_metrics["total_missing_hours"],
                    "threshold": self.quality_thresholds["max_gap_hours"],
                    "status": "warning" if quality_metrics["total_missing_hours"] > 0 else "pass",
                    "details": f"Total missing hourly readings: {quality_metrics['total_missing_hours']}"
                })

            # Sensor coverage
            sensor_coverage = quality_metrics.get("sensor_coverage", {})
            for sensor_id, metrics in sensor_coverage.items():
                battery_status = "warning" if metrics["avg_battery_level"] < 30 else "pass"
                report_data.append({
                    "category": "sensor_coverage",
                    "metric": f"{sensor_id}_battery_level",
                    "value": metrics["avg_battery_level"],
                    "threshold": 30.0,
                    "status": battery_status,
                    "details": f"Average battery: {metrics['avg_battery_level']}%, {metrics['total_readings']} readings"
                })

            # Issues summary
            for i, issue in enumerate(issues_found):
                report_data.append({
                    "category": "issues",
                    "metric": f"issue_{i+1}",
                    "value": None,
                    "threshold": None,
                    "status": "fail",
                    "details": issue
                })

            # Save to CSV
            report_df = pd.DataFrame(report_data)
            report_path = Path(self.config.paths.reports_dir) / "data_quality_report.csv"
            report_path.parent.mkdir(parents=True, exist_ok=True)

            # Add timestamp and validation metadata
            report_df["validation_timestamp"] = timestamp
            report_df["validator_version"] = "1.0.0"

            report_df.to_csv(report_path, index=False)
            self.logger.info(f"   Generated quality report: {report_path}")

        except Exception as e:
            self.logger.error(f"Failed to generate quality report: {str(e)}")

    def _log_validation_summary(self, issues_found: List[str], quality_metrics: Dict[str, Any]) -> None:
        """Log comprehensive validation statistics."""
        self.logger.info("=== Validation Summary ===")

        for key, value in self.stats.items():
            self.logger.info(f"{key.replace('_', ' ').title()}: {value}")

        self.logger.info(f"Issues Found: {len(issues_found)}")
        for issue in issues_found[:5]:  # Log first 5 issues
            self.logger.info(f"  - {issue}")

        if len(issues_found) > 5:
            self.logger.info(f"  ... and {len(issues_found) - 5} more issues")

        # Log key metrics
        overall_stats = quality_metrics.get("overall_statistics", {})
        if overall_stats:
            self.logger.info(f"Data Quality Score: {self._calculate_quality_score(quality_metrics):.1f}%")

    def _calculate_quality_score(self, quality_metrics: Dict[str, Any]) -> float:
        """
        Calculate overall data quality score (0-100).

        Args:
            quality_metrics: Quality metrics

        Returns:
            Quality score percentage
        """
        try:
            score = 100.0

            # Deduct for missing values
            missing_by_type = quality_metrics.get("missing_values_by_type", {})
            for metrics in missing_by_type.values():
                score -= min(metrics["missing_percentage"], 20)  # Max 20 point deduction

            # Deduct for anomalies
            anomalies_by_type = quality_metrics.get("anomalies_by_type", {})
            for metrics in anomalies_by_type.values():
                score -= min(metrics["anomaly_percentage"], 10)  # Max 10 point deduction

            # Deduct for gaps
            if "total_missing_hours" in quality_metrics:
                gap_penalty = min(quality_metrics["total_missing_hours"] * 2, 30)  # Max 30 point deduction
                score -= gap_penalty

            return max(score, 0.0)

        except Exception:
            return 0.0