"""
Custom exceptions for the agricultural sensor data pipeline.

These provide specific error types that can be caught and handled appropriately
by different parts of the pipeline.
"""


class PipelineError(Exception):
    """Base exception for all pipeline-related errors."""
    pass


class IngestionError(PipelineError):
    """Raised when data ingestion fails."""
    pass


class TransformationError(PipelineError):
    """Raised when data transformation fails."""
    pass


class ValidationError(PipelineError):
    """Raised when data validation fails."""
    pass


class LoadingError(PipelineError):
    """Raised when data loading/storage fails."""
    pass


class ConfigurationError(PipelineError):
    """Raised when configuration is invalid or missing."""
    pass