"""
Pydantic models for pipeline configuration.

These models provide type-safe parsing and validation of the YAML configuration file.
They ensure all required settings are present and have the correct types.
"""

from pathlib import Path
from typing import Dict, List, Optional, Union
import yaml
from pydantic import BaseModel, Field, field_validator, ConfigDict


class PipelineInfo(BaseModel):
    """Basic pipeline metadata."""
    name: str = Field(..., description="Pipeline name")
    version: str = Field(..., description="Pipeline version")


class ProjectSettings(BaseModel):
    """Project-level settings."""
    timezone: str = Field(..., description="Target timezone for data processing")
    run_id: Optional[str] = Field(None, description="Optional run identifier")


class DataPaths(BaseModel):
    """File system paths for data processing."""
    data_raw: str = Field(..., description="Directory containing raw parquet files")
    data_processed: str = Field(..., description="Directory for processed output")
    reports_dir: str = Field(..., description="Directory for quality reports")
    dq_report_csv: str = Field(..., description="Data quality report file path")

    @field_validator('data_raw', 'data_processed', 'reports_dir', 'dq_report_csv', mode='before')
    @classmethod
    def resolve_paths(cls, v):
        """Convert relative paths to absolute paths."""
        if isinstance(v, str):
            path = Path(v)
            if not path.is_absolute():
                # Resolve relative to project root (parent of src)
                project_root = Path(__file__).parent.parent.parent
                path = (project_root / v).resolve()
            return str(path)
        return v


class SchemaDefinition(BaseModel):
    """Expected data schema definition."""
    model_config = ConfigDict(
        # Avoid field name conflicts by using alias
        extra='forbid'
    )

    expected_columns: List[str] = Field(..., description="Required column names")
    types: Dict[str, str] = Field(..., description="Column name to DuckDB type mapping")


class ValueRange(BaseModel):
    """Acceptable value range for a measurement type."""
    min: float = Field(..., description="Minimum acceptable value")
    max: float = Field(..., description="Maximum acceptable value")


class CalibrationParams(BaseModel):
    """Sensor calibration parameters."""
    multiplier: float = Field(1.0, description="Calibration multiplier")
    offset: float = Field(0.0, description="Calibration offset")


class WriteSettings(BaseModel):
    """Output file writing configuration."""
    compression: str = Field("zstd", description="Compression algorithm")
    partition_by: List[str] = Field(..., description="Partitioning columns")
    mode: str = Field("overwrite", description="Write mode: overwrite or append")


class TransformationSettings(BaseModel):
    """Data transformation parameters."""
    z_score_threshold: float = Field(3.0, description="Z-score threshold for outlier detection")
    rolling_window_days: int = Field(7, description="Rolling average window in days")
    outlier_handling: str = Field("flag", description="How to handle outliers: flag or remove")


class ValidationSettings(BaseModel):
    """Data quality validation parameters."""
    max_missing_percentage: float = Field(20.0, description="Maximum acceptable missing data percentage")
    expected_frequency_hours: int = Field(1, description="Expected reading frequency in hours")


class IngestionSettings(BaseModel):
    """Data ingestion configuration."""
    incremental_mode: bool = Field(True, description="Enable incremental processing")
    checkpoint_file: str = Field(..., description="Checkpoint file path")

    @field_validator('checkpoint_file', mode='before')
    @classmethod
    def resolve_checkpoint_path(cls, v):
        """Convert relative checkpoint path to absolute path."""
        if isinstance(v, str):
            path = Path(v)
            if not path.is_absolute():
                project_root = Path(__file__).parent.parent.parent
                path = (project_root / v).resolve()
            return str(path)
        return v


class PipelineConfig(BaseModel):
    """Complete pipeline configuration model."""
    model_config = ConfigDict(
        # Allow population by alias so YAML can use "schema"
        populate_by_name=True,
        extra='forbid'
    )

    pipeline: PipelineInfo = Field(..., description="Pipeline metadata")
    project: ProjectSettings = Field(..., description="Project settings")
    paths: DataPaths = Field(..., description="File system paths")
    data_schema: SchemaDefinition = Field(..., description="Data schema definition", alias="schema")
    ranges: Dict[str, ValueRange] = Field(..., description="Acceptable value ranges per measurement type")
    calibration: Dict[str, CalibrationParams] = Field(..., description="Calibration parameters per measurement type")
    write: WriteSettings = Field(..., description="Output writing configuration")
    transformation: TransformationSettings = Field(..., description="Transformation parameters")
    validation: ValidationSettings = Field(..., description="Validation parameters")
    ingestion: IngestionSettings = Field(..., description="Ingestion configuration")

    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "PipelineConfig":
        """Load configuration from YAML file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        return cls(**config_data)

    def get_value_range(self, reading_type: str) -> Optional[ValueRange]:
        """Get value range for a specific reading type."""
        return self.ranges.get(reading_type)

    def get_calibration(self, reading_type: str) -> CalibrationParams:
        """Get calibration parameters for a specific reading type."""
        return self.calibration.get(reading_type, CalibrationParams())