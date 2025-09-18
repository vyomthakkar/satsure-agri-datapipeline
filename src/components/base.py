"""
Abstract base classes for pipeline components.

These define the interfaces that all pipeline components must implement,
ensuring consistency and enabling easy testing through dependency injection.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pathlib import Path
import pandas as pd
from src.config import PipelineConfig
from src.models import ValidationResult


class PipelineComponent(ABC):
    """Base class for all pipeline components."""

    def __init__(self, config: PipelineConfig):
        """Initialize component with pipeline configuration."""
        self.config = config

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """Execute the component's main functionality."""
        pass


class IngestionComponent(PipelineComponent):
    """Abstract base for data ingestion components."""

    @abstractmethod
    def execute(self, data_path: Optional[Path] = None) -> pd.DataFrame:
        """
        Ingest raw data from source.

        Args:
            data_path: Optional specific file path, otherwise uses config paths

        Returns:
            Raw data as pandas DataFrame
        """
        pass


class TransformationComponent(PipelineComponent):
    """Abstract base for data transformation components."""

    @abstractmethod
    def execute(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw data into cleaned, enriched format.

        Args:
            raw_data: Raw sensor data from ingestion

        Returns:
            Transformed data ready for validation
        """
        pass


class ValidationComponent(PipelineComponent):
    """Abstract base for data quality validation components."""

    @abstractmethod
    def execute(self, transformed_data: pd.DataFrame) -> pd.DataFrame:
        """
        Validate transformed data.

        Args:
            transformed_data: Transformed data from transformation component

        Returns:
            Validation results as pandas DataFrame
        """
        pass
    


class LoadingComponent(PipelineComponent):
    """Abstract base for data loading/storage components."""

    @abstractmethod
    def execute(self, validated_data: pd.DataFrame, validation_results: ValidationResult) -> bool:
        """
        Store validated data in final destination.

        Args:
            validated_data: Data that passed validation
            validation_results: Results from validation component

        Returns:
            Success status of storage operation
        """
        pass