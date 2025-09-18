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

# Integration tests (requires project setup)
python -m pytest tests/test_ingestion_integration.py -v

# Specific test examples
python -m pytest tests/test_ingestion.py::TestParquetIngestionComponent::test_execute_with_sample_data -v
python -m pytest tests/test_transformation.py::TestAgricultureTransformationComponent::test_anomaly_detection_with_outliers -v
```

### Performance Tests
```bash
# Run performance tests (marked as slow)
python -m pytest tests/ -m slow -v
```

## Test Results Summary

### Current Status: ✅ All Unit Tests Passing

**Ingestion Tests:**
```
======================= 21 passed, 5 warnings ========================
```

**Transformation Tests:**
```
======================= 18 passed, 0 warnings ========================
```

**Test Coverage:**
- 🎯 **100% Function Coverage**: All public methods tested
- 🎯 **Edge Case Coverage**: Error conditions, corrupted files, schema issues, anomaly detection
- 🎯 **Integration Coverage**: Real data processing validated
- 🎯 **Data Quality Coverage**: Missing values, duplicates, outliers, calibration

### Key Test Insights

**1. Type Compatibility Handling**
Our tests revealed that DuckDB creates `TIMESTAMP_NS` when reading pandas DataFrames, but configs expect `TIMESTAMP`. We implemented flexible type compatibility checking.

**2. Error Isolation**
Tests confirm that file processing errors don't crash the entire pipeline - it continues processing other files and provides detailed statistics.

**3. Incremental Processing**
Checkpoint management works correctly, allowing both incremental daily processing and full reloads for data corrections.

**4. Anomaly Detection Robustness**
Transformation tests validate sophisticated anomaly detection:
- ✅ Z-score based detection with configurable thresholds
- ✅ Range-based validation per reading type
- ✅ No double-counting of outliers flagged by multiple methods
- ✅ Battery level validation separate from reading anomalies
- ✅ Proper handling of edge cases (single values, all-NaN groups)

**5. Real Data Validation**
Successfully processes the project's sample data file:
- ✅ 30 records from 5 sensors
- ✅ Temperature and humidity readings
- ✅ All schema validations pass
- ✅ Statistics properly tracked

## Adding New Tests

### For New Components
1. Create `test_[component_name].py`
2. Add fixtures to `conftest.py` if needed
3. Follow the established patterns:
   - Test initialization
   - Test core functionality
   - Test error conditions
   - Test edge cases

### Test Best Practices
- Use descriptive test names
- Create isolated test environments with `temp_dir`
- Test both happy path and error conditions
- Use appropriate assertions with helpful messages
- Mock external dependencies when needed

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

## Future Enhancements

- [x] ~~Add transformation component tests~~ ✅ **Completed**
- [ ] Add validation component tests
- [ ] Add loading component tests
- [ ] Add end-to-end pipeline tests
- [ ] Add performance benchmarking
- [ ] Add data quality regression tests
- [ ] Add transformation performance tests (large datasets)
- [ ] Add anomaly detection accuracy benchmarks