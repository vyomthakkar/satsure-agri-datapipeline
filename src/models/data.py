"""
Pydantic models for data structures used throughout the pipeline.

These models ensure type safety and validation for data flowing between components.
"""

from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class SensorReading(BaseModel):
    """Model for individual sensor readings."""
    sensor_id: str = Field(..., description="Unique sensor identifier")
    timestamp: datetime = Field(..., description="Reading timestamp")
    reading_type: str = Field(..., description="Type of measurement (temperature, humidity, etc.)")
    value: float = Field(..., description="Sensor reading value")
    battery_level: float = Field(..., description="Battery level percentage (0-100)")


class ValidationResult(BaseModel):
    """Results from data quality validation."""
    passed: bool = Field(..., description="Whether validation passed")
    total_records: int = Field(..., description="Total number of records validated")
    issues_found: List[str] = Field(default_factory=list, description="List of issues discovered")
    quality_metrics: Dict[str, Any] = Field(default_factory=dict, description="Quality metrics and statistics")


class PipelineResult(BaseModel):
    """Overall result of pipeline execution."""
    success: bool = Field(..., description="Whether pipeline completed successfully")
    records_processed: int = Field(..., description="Number of records processed")
    records_stored: int = Field(..., description="Number of records successfully stored")
    validation_result: Optional[ValidationResult] = Field(None, description="Validation results")
    execution_time_seconds: float = Field(..., description="Total execution time")
    errors: List[str] = Field(default_factory=list, description="Any errors encountered")