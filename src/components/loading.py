"""
Data loading component for agricultural sensor data pipeline.

Handles optimized Parquet storage with partitioning, compression, and
quality-aware storage based on validation results.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

from src.components.base import LoadingComponent
from src.config import PipelineConfig
from src.models import ValidationResult
from src.utils import get_logger, LoadingError


class AgricultureLoadingComponent(LoadingComponent):
    """Concrete implementation of loading component for agricultural sensor data."""

    def __init__(self, config: PipelineConfig):
        """
        Initialize loading component.

        Args:
            config: Pipeline configuration
        """
        super().__init__(config)
        self.logger = get_logger(__name__)

        # Loading statistics
        self.stats = {
            "records_received": 0,
            "records_stored": 0,
            "partitions_created": 0,
            "files_written": 0,
            "storage_size_bytes": 0,
            "quality_failed_stored": 0
        }

        # Storage configuration
        self.output_path = Path(self.config.paths.data_processed)
        self.compression = self.config.write.compression
        self.partition_columns = self.config.write.partition_by
        self.write_mode = self.config.write.mode

    def execute(self, validated_data: pd.DataFrame, validation_results: ValidationResult) -> bool:
        """
        Store validated data in optimized Parquet format with partitioning.

        Args:
            validated_data: Data from validation component
            validation_results: Validation results with quality metrics

        Returns:
            True if storage was successful, False otherwise
        """
        try:
            self.logger.info("Starting data loading to processed storage")
            self.stats["records_received"] = len(validated_data)

            if validated_data.empty:
                self.logger.warning("No data to store")
                return True

            # Log validation status
            quality_status = "PASSED" if validation_results.passed else "FAILED"
            self.logger.info(f"Data quality status: {quality_status}")

            if not validation_results.passed:
                self.stats["quality_failed_stored"] = len(validated_data)
                self.logger.warning(f"Storing {len(validated_data)} records with quality issues")

            # Prepare data for storage
            prepared_data = self._prepare_data_for_storage(validated_data, validation_results)

            # Set records_stored before storage to enable proper stats calculation
            self.stats["records_stored"] = len(prepared_data)

            # Create partitioned storage structure
            success = self._store_partitioned_data(prepared_data, validation_results)

            if success:
                self._log_loading_summary()
                self.logger.info(f"Successfully stored {len(prepared_data)} records in processed storage")
                return True
            else:
                self.logger.error("Failed to store data")
                return False

        except Exception as e:
            self.logger.error(f"Loading failed: {str(e)}")
            raise LoadingError(f"Data loading failed: {str(e)}") from e

    def _prepare_data_for_storage(self, data: pd.DataFrame, validation_results: ValidationResult) -> pd.DataFrame:
        """
        Prepare data for optimized storage.

        Args:
            data: Validated data
            validation_results: Validation results

        Returns:
            Data prepared for storage
        """
        prepared_data = data.copy()

        # Ensure date column exists for partitioning
        if 'date' not in prepared_data.columns:
            prepared_data['date'] = prepared_data['timestamp'].dt.date

        # Convert date to string for partitioning (Parquet requirement)
        prepared_data['date'] = prepared_data['date'].astype(str)

        # Add metadata columns
        prepared_data['data_quality_passed'] = validation_results.passed
        prepared_data['validation_timestamp'] = datetime.now().isoformat()
        prepared_data['pipeline_version'] = self.config.pipeline.version

        # Add quality metrics summary (as JSON string for Parquet compatibility)
        if validation_results.quality_metrics:
            overall_stats = validation_results.quality_metrics.get("overall_statistics", {})
            prepared_data['quality_score'] = self._calculate_quality_score(validation_results.quality_metrics)
            prepared_data['total_issues'] = len(validation_results.issues_found)
        else:
            prepared_data['quality_score'] = 100.0
            prepared_data['total_issues'] = 0

        # Optimize data types for storage
        prepared_data = self._optimize_data_types(prepared_data)

        self.logger.info(f"Prepared {len(prepared_data)} records for storage with metadata")
        return prepared_data

    def _optimize_data_types(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize data types for efficient Parquet storage.

        Args:
            data: Data to optimize

        Returns:
            Data with optimized types
        """
        optimized_data = data.copy()

        # Convert boolean columns to efficient storage
        bool_columns = ['anomalous_reading', 'data_quality_passed']
        for col in bool_columns:
            if col in optimized_data.columns:
                optimized_data[col] = optimized_data[col].astype('bool')

        # Optimize string columns (but keep date as string for partitioning)
        categorical_columns = ['sensor_id', 'reading_type']
        for col in categorical_columns:
            if col in optimized_data.columns:
                optimized_data[col] = optimized_data[col].astype('category')

        # Ensure numeric columns have appropriate precision
        float_columns = ['value', 'battery_level', 'daily_avg_value', 'rolling_avg_value', 'quality_score']
        for col in float_columns:
            if col in optimized_data.columns:
                optimized_data[col] = pd.to_numeric(optimized_data[col], errors='coerce').astype('float32')

        # Optimize integer columns
        int_columns = ['total_issues']
        for col in int_columns:
            if col in optimized_data.columns:
                optimized_data[col] = pd.to_numeric(optimized_data[col], errors='coerce').astype('int32')

        return optimized_data

    def _store_partitioned_data(self, data: pd.DataFrame, validation_results: ValidationResult) -> bool:
        """
        Store data in partitioned Parquet format.

        Args:
            data: Prepared data for storage
            validation_results: Validation results

        Returns:
            True if storage successful, False otherwise
        """
        try:
            # Create output directory
            self.output_path.mkdir(parents=True, exist_ok=True)

            # Convert to PyArrow table for optimal Parquet handling
            table = pa.Table.from_pandas(data)

            # Create partitioning schema for modern dataset API
            partitioning = ds.partitioning(
                pa.schema([
                    (col, table.schema.field(col).type)
                    for col in self.partition_columns
                    if col in table.column_names
                ]),
                flavor="hive"  # Creates date=YYYY-MM-DD/sensor_id=X structure
            )

            self.logger.info(f"Writing partitioned data with compression: {self.compression}")

            # Configure write behavior based on mode
            if self.write_mode == 'overwrite':
                existing_data_behavior = 'delete_matching'  # Replace existing files
            else:
                existing_data_behavior = 'overwrite_or_ignore'  # Append mode

            # Write partitioned dataset using modern API
            ds.write_dataset(
                table,
                base_dir=self.output_path,
                partitioning=partitioning,
                format="parquet",
                existing_data_behavior=existing_data_behavior,
                file_options=ds.ParquetFileFormat().make_write_options(
                    compression=self.compression,
                    use_dictionary=True
                )
            )

            # Calculate storage statistics
            self._calculate_storage_stats()

            # Store validation metadata
            self._store_validation_metadata(validation_results)

            return True

        except Exception as e:
            self.logger.error(f"Failed to store partitioned data: {str(e)}")
            return False

    def _calculate_storage_stats(self) -> None:
        """Calculate and log storage statistics."""
        try:
            total_size = 0
            file_count = 0
            partition_count = 0

            # Walk through the partitioned directory structure
            for path in self.output_path.rglob("*.parquet"):
                total_size += path.stat().st_size
                file_count += 1

            # Count unique partitions dynamically based on first partition column
            # This works for any partition configuration (date, sensor_id, reading_type, etc.)
            partition_dirs = set()
            if self.partition_columns:
                first_partition_col = self.partition_columns[0]
                partition_pattern = f"{first_partition_col}=*"
                for path in self.output_path.rglob(partition_pattern):
                    if path.is_dir():
                        partition_dirs.add(path)
            partition_count = len(partition_dirs)

            self.stats.update({
                "storage_size_bytes": total_size,
                "files_written": file_count,
                "partitions_created": partition_count
            })

            # Log storage efficiency
            if self.stats["records_stored"] > 0:
                bytes_per_record = total_size / self.stats["records_stored"]
                self.logger.info(f"Storage efficiency: {bytes_per_record:.2f} bytes/record")

        except Exception as e:
            self.logger.warning(f"Failed to calculate storage stats: {str(e)}")

    def _store_validation_metadata(self, validation_results: ValidationResult) -> None:
        """
        Store validation metadata alongside the data.

        Args:
            validation_results: Validation results to store
        """
        try:
            metadata_path = self.output_path / "_validation_metadata.json"

            metadata = {
                "validation_timestamp": datetime.now().isoformat(),
                "data_quality_passed": validation_results.passed,
                "total_records": validation_results.total_records,
                "issues_count": len(validation_results.issues_found),
                "issues_found": validation_results.issues_found,
                "quality_metrics": validation_results.quality_metrics,
                "pipeline_version": self.config.pipeline.version,
                "storage_stats": self.stats.copy()
            }

            import json
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)

            self.logger.info(f"Stored validation metadata: {metadata_path}")

        except Exception as e:
            self.logger.warning(f"Failed to store validation metadata: {str(e)}")

    def _calculate_quality_score(self, quality_metrics: Dict[str, Any]) -> float:
        """
        Calculate a simple quality score from validation metrics.

        Args:
            quality_metrics: Quality metrics from validation

        Returns:
            Quality score (0-100)
        """
        try:
            score = 100.0

            # Deduct for missing values
            missing_by_type = quality_metrics.get("missing_values_by_type", {})
            for metrics in missing_by_type.values():
                score -= min(metrics["missing_percentage"], 20)

            # Deduct for anomalies
            anomalies_by_type = quality_metrics.get("anomalies_by_type", {})
            for metrics in anomalies_by_type.values():
                score -= min(metrics["anomaly_percentage"], 10)

            # Deduct for gaps
            if "total_missing_hours" in quality_metrics:
                gap_penalty = min(quality_metrics["total_missing_hours"] * 2, 30)
                score -= gap_penalty

            return max(score, 0.0)

        except Exception:
            return 50.0  # Default score if calculation fails

    def query_stored_data(self, date_filter: Optional[str] = None, sensor_filter: Optional[str] = None) -> pd.DataFrame:
        """
        Query stored data with optional filters (utility method for testing/analysis).

        Args:
            date_filter: Date filter in YYYY-MM-DD format
            sensor_filter: Sensor ID filter

        Returns:
            Filtered data from storage
        """
        try:
            # Simple approach: just read all data and filter in pandas for this utility method
            # For production use, you'd want more sophisticated partition-aware filtering
            dataset = ds.dataset(self.output_path, format="parquet")
            df = dataset.to_table().to_pandas()

            # Apply filters in pandas - this works reliably across all partition schemes
            if date_filter and 'date' in df.columns:
                df = df[df['date'] == date_filter]
            elif date_filter:
                # Date is partition column, need to reconstruct from timestamp if available
                if 'timestamp' in df.columns:
                    df['temp_date'] = pd.to_datetime(df['timestamp']).dt.date.astype(str)
                    df = df[df['temp_date'] == date_filter]
                    df = df.drop('temp_date', axis=1)

            if sensor_filter and 'sensor_id' in df.columns:
                df = df[df['sensor_id'] == sensor_filter]

            return df

        except Exception as e:
            self.logger.error(f"Failed to query stored data: {str(e)}")
            return pd.DataFrame()

    def get_storage_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive storage summary.

        Returns:
            Dictionary with storage statistics and partition information
        """
        try:
            summary = {
                "storage_path": str(self.output_path),
                "compression": self.compression,
                "partition_columns": self.partition_columns,
                "storage_stats": self.stats.copy(),
                "partitions": []
            }

            # Collect partition information - maintain backward compatibility for date/sensor_id structure
            if self.partition_columns == ["date", "sensor_id"]:
                self._collect_date_sensor_partitions(summary["partitions"])
            else:
                # Generic dynamic partition collection for other configurations
                self._collect_partition_info_recursive(self.output_path, self.partition_columns, {}, summary["partitions"])

            return summary

        except Exception as e:
            self.logger.error(f"Failed to get storage summary: {str(e)}")
            return {"error": str(e)}

    def _collect_partition_info_recursive(self, current_path: Path, remaining_columns: list,
                                         current_partition: dict, partitions_list: list) -> None:
        """
        Recursively collect partition information for any partition column configuration.

        Args:
            current_path: Current directory path
            remaining_columns: Partition columns left to process
            current_partition: Current partition info being built
            partitions_list: List to append completed partition info to
        """
        if not remaining_columns:
            # Base case: no more partition columns, collect files
            parquet_files = list(current_path.glob("*.parquet"))
            if parquet_files:
                current_partition["files"] = len(parquet_files)
                current_partition["size_bytes"] = sum(f.stat().st_size for f in parquet_files)
                partitions_list.append(current_partition.copy())
            return

        # Process next partition column
        partition_col = remaining_columns[0]
        remaining_cols = remaining_columns[1:]

        # Find directories matching this partition column pattern
        pattern = f"{partition_col}=*"
        for partition_dir in sorted(current_path.glob(pattern)):
            if partition_dir.is_dir():
                # Extract partition value
                partition_value = partition_dir.name.split("=")[1]
                current_partition[partition_col] = partition_value

                # Recurse into subdirectories
                self._collect_partition_info_recursive(
                    partition_dir, remaining_cols, current_partition, partitions_list
                )

    def _collect_date_sensor_partitions(self, partitions_list: list) -> None:
        """
        Collect partition information in backward-compatible date/sensor format.

        Args:
            partitions_list: List to append partition info to
        """
        for date_dir in sorted(self.output_path.glob("date=*")):
            if date_dir.is_dir():
                partition_info = {
                    "date": date_dir.name.split("=")[1],
                    "sensors": []
                }

                for sensor_dir in sorted(date_dir.glob("sensor_id=*")):
                    if sensor_dir.is_dir():
                        sensor_id = sensor_dir.name.split("=")[1]
                        parquet_files = list(sensor_dir.glob("*.parquet"))
                        total_size = sum(f.stat().st_size for f in parquet_files)

                        partition_info["sensors"].append({
                            "sensor_id": sensor_id,
                            "files": len(parquet_files),
                            "size_bytes": total_size
                        })

                partitions_list.append(partition_info)

    def _log_loading_summary(self) -> None:
        """Log comprehensive loading statistics."""
        self.logger.info("=== Loading Summary ===")

        for key, value in self.stats.items():
            if key == "storage_size_bytes":
                # Convert bytes to human readable format
                if value > 1024 * 1024:
                    size_mb = value / (1024 * 1024)
                    self.logger.info(f"Storage Size: {size_mb:.2f} MB")
                elif value > 1024:
                    size_kb = value / 1024
                    self.logger.info(f"Storage Size: {size_kb:.2f} KB")
                else:
                    self.logger.info(f"Storage Size: {value} bytes")
            else:
                self.logger.info(f"{key.replace('_', ' ').title()}: {value}")

        # Log storage efficiency metrics
        if self.stats["records_stored"] > 0:
            efficiency = (self.stats["records_stored"] / self.stats["records_received"]) * 100
            self.logger.info(f"Storage Efficiency: {efficiency:.1f}%")

        # Log partition structure
        self.logger.info(f"Output Location: {self.output_path}")
        self.logger.info(f"Compression: {self.compression}")
        self.logger.info(f"Partitioned By: {', '.join(self.partition_columns)}")