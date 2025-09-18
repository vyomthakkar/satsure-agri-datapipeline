# Test Suite Documentation

## Overview

This directory contains comprehensive tests for the agricultural sensor data pipeline, focusing on robust testing of all pipeline components with both synthetic and real data.

## Test Structure

### Unit Tests (`test_ingestion.py`)
Comprehensive unit tests for the ingestion component covering:

**Core Functionality:**
- ✅ Component initialization and configuration
- ✅ File discovery (full and incremental modes)
- ✅ Checkpoint management (load, save, corrupted files)
- ✅ Data processing with DuckDB validation

**Schema Validation:**
- ✅ Valid schema processing
- ✅ Missing column detection
- ✅ Extra column rejection
- ✅ Column order flexibility (warnings, not errors)
- ✅ Type compatibility (TIMESTAMP vs TIMESTAMP_NS)

**Error Handling:**
- ✅ Corrupted file handling
- ✅ Missing raw data directory
- ✅ Schema mismatches
- ✅ Error isolation (continue processing other files)

**Statistics & Logging:**
- ✅ Ingestion statistics tracking
- ✅ Success rate calculations
- ✅ Comprehensive logging

**Configuration Features:**
- ✅ Incremental vs full reload modes
- ✅ Force full reload override
- ✅ Specific file processing

### Unit Tests (`test_transformation.py`)
Comprehensive unit tests for the transformation component covering:

**Core Functionality:**
- ✅ Component initialization and configuration
- ✅ Data cleaning (duplicate removal, missing value handling)
- ✅ Sensor calibration with multiplier/offset
- ✅ Timezone conversion (UTC to target timezone)
- ✅ Derived fields calculation (daily averages, rolling averages)

**Anomaly Detection:**
- ✅ Z-score based outlier detection (configurable threshold)
- ✅ Range-based anomaly detection (per reading type)
- ✅ Outlier handling modes (flag vs remove)
- ✅ Battery level validation (separate from reading anomalies)
- ✅ Statistics tracking without double-counting

**Data Quality:**
- ✅ Empty DataFrame handling
- ✅ Missing critical field validation
- ✅ Battery level imputation (group median with fallback)
- ✅ Transformation statistics tracking
- ✅ Data retention rate calculation

**Error Handling:**
- ✅ Invalid data structure handling
- ✅ TransformationError propagation
- ✅ Comprehensive logging and summary reporting

### Unit Tests (`test_validation.py`)
Comprehensive unit tests for the validation component covering:

**Core Functionality:**
- ✅ Component initialization and configuration
- ✅ Data quality validation with configurable thresholds
- ✅ Value range validation per reading type
- ✅ Gap detection using time series analysis
- ✅ Quality score calculation and reporting

**Schema Validation:**
- ✅ Required column presence validation
- ✅ Data type compatibility checking
- ✅ Missing value percentage thresholds
- ✅ Anomaly rate threshold validation

**Quality Metrics:**
- ✅ Missing value analysis by reading type
- ✅ Anomaly percentage calculation
- ✅ Sensor coverage analysis
- ✅ Gap detection with configurable thresholds
- ✅ Overall statistics generation

**Report Generation:**
- ✅ Quality report CSV export
- ✅ Validation metadata JSON storage
- ✅ Issue tracking and categorization
- ✅ Statistics tracking and logging

**Error Handling:**
- ✅ Empty data handling
- ✅ Missing column scenarios
- ✅ ValidationError propagation
- ✅ Quality threshold failures

### Unit Tests (`test_loading.py`)
Comprehensive unit tests for the loading component covering:

**Core Functionality:**
- ✅ Component initialization and configuration
- ✅ Partitioned Parquet storage (Hive-style partitioning)
- ✅ Data type optimization and compression
- ✅ Validation metadata storage
- ✅ Storage statistics tracking

**Storage Features:**
- ✅ Modern PyArrow dataset API usage
- ✅ ZSTD compression and optimization
- ✅ Quality-aware data storage
- ✅ Overwrite vs append mode handling
- ✅ Concurrent storage access safety

**Data Management:**
- ✅ Partitioned data querying
- ✅ Storage summary generation
- ✅ Quality score integration
- ✅ Large dataset handling
- ✅ Empty data scenarios

**Error Handling:**
- ✅ LoadingError exception handling
- ✅ Storage failure scenarios
- ✅ Invalid data structure handling
- ✅ Configuration validation

### Integration Tests (`test_ingestion_integration.py`)
Real-world integration tests:

**Real Data Processing:**
- 🔧 Real sample data validation (requires project setup)
- 🔧 Performance benchmarking
- 🔧 Configuration validation

### Test Fixtures (`conftest.py`)
Comprehensive test fixtures providing:

**Configuration:**
- `sample_config`: Test configuration with temporary paths
- `real_config`: Actual project configuration

**Test Data:**
- `sample_sensor_data`: Valid sensor readings
- `invalid_schema_data`: Data with missing columns
- `wrong_types_data`: Data with incorrect types
- `sample_parquet_files`: Pre-created test files

**Utilities:**
- `temp_dir`: Temporary directories for isolated tests
- `create_test_parquet_file()`: Helper for creating test files

## Running Tests

### All Tests
```bash
# Run complete test suite
python -m pytest tests/ -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

### Specific Test Modules
```bash
# Ingestion tests
python -m pytest tests/test_ingestion.py -v

# Transformation tests
python -m pytest tests/test_transformation.py -v

# Validation tests
python -m pytest tests/test_validation.py -v

# Loading tests
python -m pytest tests/test_loading.py -v

# Integration tests (requires project setup)
python -m pytest tests/test_ingestion_integration.py -v

# Specific test examples
python -m pytest tests/test_ingestion.py::TestParquetIngestionComponent::test_execute_with_sample_data -v
python -m pytest tests/test_transformation.py::TestAgricultureTransformationComponent::test_anomaly_detection_with_outliers -v
python -m pytest tests/test_validation.py::TestAgricultureValidationComponent::test_execute_with_sample_data -v
python -m pytest tests/test_loading.py::TestAgricultureLoadingComponent::test_execute_with_valid_data -v
```

### Performance Tests
```bash
# Run performance tests (marked as slow)
python -m pytest tests/ -m slow -v
```


## CI/CD Integration

This test suite is designed for continuous integration:

```yaml
# Example CI configuration
test:
  script:
    - pip install -r requirements.txt
    - python -m pytest tests/test_ingestion.py --cov=src
    - python -m pytest tests/test_transformation.py --cov=src
    - python -m pytest tests/test_ingestion_integration.py || true  # Allow failure if no real data
```

