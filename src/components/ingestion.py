"""
Data ingestion component for agricultural sensor data pipeline.

Handles reading Parquet files from raw data directory with DuckDB validation,
checkpoint management, and comprehensive error handling.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
import duckdb
import pandas as pd

from src.components.base import IngestionComponent
from src.config import PipelineConfig
from src.utils import get_logger, IngestionError


class ParquetIngestionComponent(IngestionComponent):
    """Concrete implementation of ingestion component for Parquet files."""

    def __init__(self, config: PipelineConfig):
        """
        Initialize ingestion component.

        Args:
            config: Pipeline configuration
        """
        super().__init__(config)
        self.logger = get_logger(__name__)
        self.duckdb_conn = duckdb.connect(':memory:')

        # Initialize ingestion statistics
        self.stats = {
            "files_discovered": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "records_ingested": 0,
            "records_skipped": 0
        }

    def execute(self, data_path: Optional[Path] = None, force_full_reload: bool = False) -> pd.DataFrame:
        """
        Execute data ingestion from Parquet files.

        Args:
            data_path: Optional specific file path to process
            force_full_reload: Override incremental mode to reprocess all files

        Returns:
            Combined DataFrame of ingested data

        Raises:
            IngestionError: If ingestion fails
        """
        try:
            self.logger.info("Starting data ingestion")

            # Determine processing mode
            use_incremental = self.config.ingestion.incremental_mode and not force_full_reload
            mode_desc = "incremental" if use_incremental else "full reload"
            self.logger.info(f"Processing mode: {mode_desc}")

            # Discover files to process
            if data_path:
                files_to_process = [data_path]
                self.logger.info(f"Processing specific file: {data_path}")
            else:
                files_to_process = self._discover_files(use_incremental)

            self.stats["files_discovered"] = len(files_to_process)
            self.logger.info(f"Discovered {len(files_to_process)} files to process")

            # Process files
            all_data = []
            for file_path in files_to_process:
                try:
                    file_data = self._process_file(file_path)
                    if file_data is not None and not file_data.empty:
                        all_data.append(file_data)
                        self.stats["files_processed"] += 1
                        self.stats["records_ingested"] += len(file_data)
                        self.logger.info(f"Successfully processed {file_path.name}: {len(file_data)} records")
                    else:
                        self.stats["files_skipped"] += 1
                        self.logger.warning(f"Skipped {file_path.name}: no valid data")

                except Exception as e:
                    self.stats["files_failed"] += 1
                    self.logger.error(f"Failed to process {file_path.name}: {str(e)}")
                    # Continue processing other files

            # Combine all data
            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
                self.logger.info(f"Combined {len(combined_data)} total records from {len(all_data)} files")
            else:
                combined_data = pd.DataFrame()
                self.logger.warning("No data was successfully ingested")

            # Update checkpoint (only if using incremental mode)
            if use_incremental and files_to_process:
                self._update_checkpoint(files_to_process)

            # Log final statistics
            self._log_ingestion_summary()

            return combined_data

        except Exception as e:
            self.logger.error(f"Ingestion failed: {str(e)}")
            raise IngestionError(f"Data ingestion failed: {str(e)}") from e

    def _discover_files(self, use_incremental: bool) -> List[Path]:
        """
        Discover Parquet files to process.

        Args:
            use_incremental: Whether to use incremental processing

        Returns:
            List of file paths to process
        """
        raw_data_path = Path(self.config.paths.data_raw)

        if not raw_data_path.exists():
            raise IngestionError(f"Raw data directory does not exist: {raw_data_path}")

        # Find all parquet files
        all_files = sorted(raw_data_path.glob("*.parquet"))

        if not all_files:
            self.logger.warning(f"No parquet files found in {raw_data_path}")
            return []

        if not use_incremental:
            self.logger.info(f"Full reload mode: processing all {len(all_files)} files")
            return all_files

        # Incremental mode: filter based on checkpoint
        processed_files = self._load_checkpoint()
        new_files = [f for f in all_files if f.name not in processed_files]

        self.logger.info(f"Incremental mode: {len(new_files)} new files, {len(processed_files)} already processed")
        return new_files

    def _load_checkpoint(self) -> Set[str]:
        """
        Load checkpoint data to track processed files.

        Returns:
            Set of processed filenames
        """
        checkpoint_path = Path(self.config.ingestion.checkpoint_file)

        if not checkpoint_path.exists():
            self.logger.info("No checkpoint file found, starting fresh")
            return set()

        try:
            with open(checkpoint_path, 'r') as f:
                checkpoint_data = json.load(f)

            processed_files = set(checkpoint_data.get("processed_files", []))
            last_update = checkpoint_data.get("last_update")

            self.logger.info(f"Loaded checkpoint: {len(processed_files)} files processed, last update: {last_update}")
            return processed_files

        except Exception as e:
            self.logger.warning(f"Failed to load checkpoint: {e}. Starting fresh.")
            return set()

    def _update_checkpoint(self, processed_files: List[Path]) -> None:
        """
        Update checkpoint file with newly processed files.

        Args:
            processed_files: List of files that were processed in this run
        """
        checkpoint_path = Path(self.config.ingestion.checkpoint_file)

        # Load existing checkpoint
        existing_files = self._load_checkpoint()

        # Add newly processed files
        new_filenames = {f.name for f in processed_files}
        all_processed = existing_files.union(new_filenames)

        # Save updated checkpoint
        checkpoint_data = {
            "processed_files": list(all_processed),
            "last_update": datetime.now().isoformat(),
            "last_run_stats": self.stats.copy()
        }

        try:
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            with open(checkpoint_path, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)

            self.logger.info(f"Updated checkpoint: {len(all_processed)} total files processed")

        except Exception as e:
            self.logger.error(f"Failed to update checkpoint: {e}")

    def _process_file(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Process a single Parquet file with validation.

        Args:
            file_path: Path to the Parquet file

        Returns:
            DataFrame with file data, or None if processing failed
        """
        try:
            # Use DuckDB to read Parquet and inspect schema without full scan
            rel = self.duckdb_conn.read_parquet(file_path.as_posix())
            actual_cols = list(rel.columns)
            actual_types_list = [str(t).upper() for t in rel.types]
            actual_type_map = dict(zip(actual_cols, actual_types_list))

            expected_cols = list(self.config.schema.expected_columns)
            expected_type_map = {k: v.upper() for k, v in self.config.schema.types.items()}

            # Validate presence of expected columns and check for extras
            missing = [c for c in expected_cols if c not in actual_cols]
            extra = [c for c in actual_cols if c not in expected_cols]
            if missing or extra:
                self.logger.error(
                    f"{file_path.name} schema mismatch. Missing: {missing or []}, Extra: {extra or []}"
                )
                return None

            # Warn if column order differs (do not fail unless strict order is required)
            if actual_cols != expected_cols:
                self.logger.warning(
                    f"{file_path.name} column order differs. Expected {expected_cols}, found {actual_cols}"
                )

            # Validate types by column name
            type_mismatches = {
                c: {"expected": expected_type_map.get(c), "actual": actual_type_map.get(c)}
                for c in expected_cols
                if expected_type_map.get(c) != actual_type_map.get(c)
            }
            if type_mismatches:
                self.logger.error(f"{file_path.name} type mismatches: {type_mismatches}")
                return None

            # Load validated data to pandas DataFrame
            df = rel.df()
            return df

        except Exception as e:
            self.logger.error(f"Error processing {file_path.name}: {e}")
            return None
        

    def _log_ingestion_summary(self) -> None:
        """Log comprehensive ingestion statistics."""
        self.logger.info("=== Ingestion Summary ===")
        for key, value in self.stats.items():
            self.logger.info(f"{key.replace('_', ' ').title()}: {value}")

        # Calculate success rate
        if self.stats["files_discovered"] > 0:
            success_rate = (self.stats["files_processed"] / self.stats["files_discovered"]) * 100
            self.logger.info(f"Success Rate: {success_rate:.1f}%")