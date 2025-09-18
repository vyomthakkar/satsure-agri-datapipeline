"""
Main pipeline orchestrator for agricultural sensor data processing.

This module coordinates the execution of all pipeline components:
ingestion -> transformation -> validation -> loading
"""

import time
from pathlib import Path
from typing import Optional

from src.config import PipelineConfig
from src.models import PipelineResult, ValidationResult
from src.components import (
    ParquetIngestionComponent,
    AgricultureTransformationComponent,
    AgricultureValidationComponent,
    AgricultureLoadingComponent
)


class AgricultureDataPipeline:
    """Main pipeline orchestrator that coordinates all components."""

    def __init__(self, config: PipelineConfig):
        """
        Initialize pipeline with configuration.

        Args:
            config: Pipeline configuration loaded from YAML
        """
        self.config = config

        # Components will be injected (dependency injection pattern)
        self.ingestion: Optional[ParquetIngestionComponent] = None
        self.transformation: Optional[AgricultureTransformationComponent] = None
        self.validation: Optional[AgricultureValidationComponent] = None
        self.loading: Optional[AgricultureLoadingComponent] = None

    def set_components(
        self,
        ingestion: ParquetIngestionComponent,
        transformation: AgricultureTransformationComponent,
        validation: AgricultureValidationComponent,
        loading: AgricultureLoadingComponent
    ):
        """
        Set pipeline components (dependency injection).

        Args:
            ingestion: Data ingestion component
            transformation: Data transformation component
            validation: Data validation component
            loading: Data loading component
        """
        self.ingestion = ingestion
        self.transformation = transformation
        self.validation = validation
        self.loading = loading

    def execute(self, input_path: Optional[Path] = None) -> PipelineResult:
        """
        Execute the complete data pipeline.

        Args:
            input_path: Optional specific input file path

        Returns:
            Pipeline execution results
        """
        start_time = time.time()
        errors = []

        try:
            # Validate components are set
            if not all([self.ingestion, self.transformation, self.validation, self.loading]):
                raise ValueError("All pipeline components must be set before execution")

            print(f"Starting pipeline: {self.config.pipeline.name}")

            # Step 1: Data Ingestion
            print("Step 1: Data Ingestion")
            raw_data = self.ingestion.execute(input_path)
            print(f"   Ingested {len(raw_data)} records")

            # Step 2: Data Transformation
            print("Step 2: Data Transformation")
            transformed_data = self.transformation.execute(raw_data)
            print(f"   Transformed {len(transformed_data)} records")

            # Step 3: Data Validation
            print("Step 3: Data Validation")
            validation_results = self.validation.execute(transformed_data)
            print(f"   Validation completed")

            # Step 4: Data Loading
            print("Step 4: Data Loading")
            loading_success = self.loading.execute(transformed_data, validation_results)

            if loading_success:
                print(f"   Successfully stored {len(transformed_data)} records")
            else:
                errors.append("Data loading failed")

            # Calculate results
            execution_time = time.time() - start_time

            return PipelineResult(
                success=loading_success and len(errors) == 0,
                records_processed=len(raw_data),
                records_stored=len(transformed_data) if loading_success else 0,
                validation_result=validation_results,
                execution_time_seconds=execution_time,
                errors=errors
            )

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Pipeline execution failed: {str(e)}"
            errors.append(error_msg)
            print(f"{error_msg}")

            return PipelineResult(
                success=False,
                records_processed=0,
                records_stored=0,
                validation_result=None,
                execution_time_seconds=execution_time,
                errors=errors
            )


def main():
    """Main entry point for pipeline execution."""
    try:
        # Load configuration
        config_path = Path("config/default.yaml")
        config = PipelineConfig.from_yaml(config_path)

        # Create pipeline and components
        pipeline = AgricultureDataPipeline(config)

        # Initialize all components
        ingestion = ParquetIngestionComponent(config)
        transformation = AgricultureTransformationComponent(config)
        validation = AgricultureValidationComponent(config)
        loading = AgricultureLoadingComponent(config)

        # Set components in pipeline
        pipeline.set_components(ingestion, transformation, validation, loading)

        # Execute pipeline
        result = pipeline.execute()

        print(f"\n📊 Pipeline Execution Summary:")
        print(f"   Success: {result.success}")
        print(f"   Records processed: {result.records_processed}")
        print(f"   Records stored: {result.records_stored}")
        print(f"   Execution time: {result.execution_time_seconds:.2f} seconds")

        if result.errors:
            print(f"   Errors: {', '.join(result.errors)}")

        if result.validation_result:
            print(f"   Validation passed: {result.validation_result.passed}")
            print(f"   Issues found: {len(result.validation_result.issues_found)}")

    except Exception as e:
        print(f"Failed to initialize pipeline: {e}")


if __name__ == "__main__":
    main()