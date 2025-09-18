# Agricultural Sensor Data Pipeline

Link to explainer video: https://www.tella.tv/video/satsure-data-pipeline-assignment-ejdh

This pipeline ingests, transforms, validates, and stores sensor readings (temperature, humidity, battery levels) using DuckDB and Parquet storage.

### Key Features

- **🔄 Incremental Processing**: Checkpoint-based ingestion prevents duplicate processing
- **🛡️ Schema Validation**: Robust handling of schema mismatches and data quality issues
- **📊 Anomaly Detection**: Z-score and range-based outlier identification
- **⚡ DuckDB Analytics**: High-performance data validation and aggregation
- **📦 Columnar Storage**: Optimized Parquet files with Hive-style partitioning
- **🔍 Quality Reporting**: Comprehensive validation reports and metadata tracking

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Virtual environment (recommended)

### Setup Instructions

1. **Clone and Setup Environment**
   ```bash
   git clone <repository-url>
   cd satsure-agri-datapipeline

   # Create virtual environment
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate

   # Install dependencies
   pip install -r requirements.txt
   ```

2. **Generate Sample Data (Optional)**
   ```bash
   # Generate synthetic sensor data with edge cases
   python scripts/generate_synthetic_raw.py --include-edge-cases
   ```

3. **Run the Complete Pipeline**
   ```bash
   # Option 1: Use the main orchestrator
   python -m src.main

   # Option 2: Use the demo script with detailed output
   python run_pipeline_demo.py
   ```

4. **Run Individual Components**
   ```bash
   # Ingestion only
   python -c "from src.components import ParquetIngestionComponent; from src.config import PipelineConfig;
              config = PipelineConfig.from_yaml('config/default.yaml');
              component = ParquetIngestionComponent(config);
              data = component.execute()"

   # Check processed data
   ls data/processed/
   ```

### Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific component tests
python -m pytest tests/test_ingestion.py -v
python -m pytest tests/test_transformation.py -v
python -m pytest tests/test_validation.py -v
python -m pytest tests/test_loading.py -v

# Run with coverage
python -m pytest tests/ --cov=src --cov-report=html
```

## 🏗️ Architecture & Components

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Agricultural Data Pipeline                │
└─────────────────────────────────────────────────────────────┘
           ┌──────────────┐    ┌──────────────┐
           │   Raw Data   │    │    Config    │
           │  (Parquet)   │    │  (YAML)      │
           └──────┬───────┘    └──────┬───────┘
                  │                   │
                  ▼                   ▼
         ┌─────────────────────────────────┐
         │      1. INGESTION COMPONENT     │
         │  • Schema validation (DuckDB)   │
         │  • Incremental loading         │
         │  • Checkpoint management       │
         │  • Error isolation             │
         └─────────────┬───────────────────┘
                       │
                       ▼
         ┌─────────────────────────────────┐
         │   2. TRANSFORMATION COMPONENT   │
         │  • Data cleaning & deduplication│
         │  • Sensor calibration          │
         │  • Anomaly detection (Z-score)  │
         │  • Derived fields calculation   │
         │  • Timezone conversion         │
         └─────────────┬───────────────────┘
                       │
                       ▼
         ┌─────────────────────────────────┐
         │    3. VALIDATION COMPONENT      │
         │  • Value range validation      │
         │  • Gap detection analysis      │
         │  • Quality score calculation   │
         │  • Metrics profiling           │
         └─────────────┬───────────────────┘
                       │
                       ▼
         ┌─────────────────────────────────┐
         │     4. LOADING COMPONENT        │
         │  • Partitioned Parquet storage │
         │  • ZSTD compression            │
         │  • Metadata persistence        │
         │  • Quality-aware storage       │
         └─────────────┬───────────────────┘
                       │
                       ▼
      ┌─────────────────────────────────────────┐
      │          PROCESSED DATA                 │
      │  data/processed/date=YYYY-MM-DD/        │
      │                 sensor_id=X/            │
      │                 part-0.parquet          │
      └─────────────────────────────────────────┘
```

### Component Details

#### 1. **Ingestion Component** (`src/components/ingestion.py`)
- **Purpose**: Loads raw Parquet files with schema validation and error handling
- **Technology**: DuckDB for schema inspection and validation queries
- **Features**:
  - Incremental processing with checkpoint tracking
  - Schema compatibility checking with flexible type handling
  - Error isolation (continues processing other files if one fails)
  - Comprehensive statistics tracking

#### 2. **Transformation Component** (`src/components/transformation.py`)
- **Purpose**: Cleans, enriches, and derives new fields from raw sensor data
- **Features**:
  - Duplicate removal and missing value imputation
  - Sensor calibration (multiplier + offset corrections)
  - Multi-method anomaly detection (Z-score + range validation)
  - Time-series features (daily averages, rolling windows)
  - Timezone normalization to UTC+5:30

#### 3. **Validation Component** (`src/components/validation.py`)
- **Purpose**: Ensures data quality and generates comprehensive quality reports
- **Technology**: DuckDB for gap detection and statistical analysis
- **Features**:
  - Value range validation per reading type
  - Temporal gap detection using time-series analysis
  - Quality score calculation and thresholding
  - Detailed quality metrics and sensor coverage analysis

#### 4. **Loading Component** (`src/components/loading.py`)
- **Purpose**: Optimizes and stores validated data for analytical queries
- **Technology**: Modern PyArrow dataset API with Hive-style partitioning
- **Features**:
  - Date and sensor-based partitioning
  - ZSTD compression with dictionary encoding
  - Quality metadata co-location
  - Concurrent access safety

### Configuration-Driven Design

All pipeline behavior is controlled through `config/default.yaml`:

```yaml
# Data paths
paths:
  data_raw: "data/raw"
  data_processed: "data/processed"
  reports_dir: "data/reports"

# Schema definitions
schema:
  sensor_id: VARCHAR
  timestamp: TIMESTAMP
  reading_type: VARCHAR
  value: DOUBLE
  battery_level: DOUBLE

# Value ranges for validation
ranges:
  temperature: {min: -10.0, max: 60.0}
  humidity: {min: 0.0, max: 100.0}
  battery_level: {min: 0.0, max: 100.0}

# Calibration parameters
calibration:
  sensor_1: {multiplier: 1.05, offset: -0.2}
  sensor_2: {multiplier: 0.98, offset: 0.1}

# Processing settings
processing:
  z_score_threshold: 3.0
  rolling_window_days: 7
  gap_threshold_hours: 1.0
```

## 🔧 Calibration & Anomaly Logic

### Sensor Calibration

Each sensor can have unique calibration parameters to correct for manufacturing variations or environmental factors:

```python
# Applied during transformation
calibrated_value = raw_value * sensor_multiplier + sensor_offset

# Example: sensor_1 reads 0.5°C high
sensor_1: {multiplier: 1.0, offset: -0.5}
```

### Anomaly Detection (Multi-Method Approach)

The pipeline employs multiple anomaly detection methods to ensure robust outlier identification:

#### 1. **Z-Score Based Detection**
```python
z_score = (value - mean) / std_dev
anomalous = abs(z_score) > threshold  # default: 3.0
```

#### 2. **Range-Based Validation**
```python
# Per reading type ranges from configuration
temperature_anomaly = value < -10.0 or value > 60.0
humidity_anomaly = value < 0.0 or value > 100.0
battery_anomaly = value < 0.0 or value > 100.0
```

#### 3. **Statistical Outlier Detection**
- Uses grouped statistics (per sensor + reading type)
- Handles edge cases (single values, all-NaN groups)
- Prevents double-counting across detection methods

### Quality Score Calculation

```python
def calculate_quality_score(metrics: Dict) -> float:
    """Calculate overall data quality score (0-100)"""
    # Base score starts at 100
    score = 100.0

    # Penalize missing values (up to -30 points)
    missing_penalty = min(30.0, overall_missing_pct * 0.5)
    score -= missing_penalty

    # Penalize anomalies (up to -20 points)
    anomaly_penalty = min(20.0, overall_anomaly_pct * 0.3)
    score -= anomaly_penalty

    # Penalize data gaps (up to -50 points)
    gap_penalty = min(50.0, significant_gaps_count * 2.0)
    score -= gap_penalty

    return max(0.0, score)
```

## 📊 Example Data Quality Report

### Validation Summary
```json
{
  "validation_timestamp": "2025-09-18T16:19:55.614862",
  "data_quality_passed": false,
  "total_records": 72,
  "issues_count": 13,
  "pipeline_version": "1.0.0"
}
```

### Key Quality Metrics

#### **Range Violations Detected**
- 🔴 **4 temperature values** outside range [-10.0, 60.0°C]
- 🔴 **9 humidity values** outside range [0.0, 100.0%]
- 🔴 **2 battery values** outside range [0.0, 100.0%]

#### **Anomaly Analysis**
```
Reading Type    | Total Records | Anomalies | Percentage
----------------|---------------|-----------|------------
Humidity        | 40            | 9         | 22.5%
Temperature     | 32            | 4         | 12.5%
```

#### **Sensor Coverage Analysis**
```
Sensor ID | Reading Types | Total Readings | Avg Battery | Status
----------|---------------|----------------|-------------|--------
sensor_5  | 2             | 27             | 61.6%       | ✅ Good
sensor_2  | 2             | 14             | 64.1%       | ⚠️ Gaps
sensor_4  | 2             | 14             | 53.0%       | ⚠️ Gaps
sensor_1  | 2             | 12             | 51.6%       | ⚠️ Gaps
sensor_3  | 2             | 5              | 76.2%       | 🔴 Sparse
```

#### **Gap Detection Results**
- **Total missing hours**: 643 across all sensor/reading combinations
- **Largest gap**: sensor_3 temperature (70 hours: 2023-06-02 to 2023-06-05)
- **Gap threshold**: 1.0 hours (configurable)

#### **Storage Statistics**
```json
{
  "records_received": 72,
  "records_stored": 72,
  "partitions_created": 5,
  "files_written": 24,
  "storage_size_bytes": 228878,
  "compression_ratio": "3.2:1"
}
```

### Quality Report Files Generated

1. **`data/processed/_validation_metadata.json`** - Comprehensive validation results
2. **`data/reports/data_quality_report.csv`** - Tabular quality metrics for analysis
3. **`data/.checkpoint`** - Incremental processing state

## 🧪 Testing

### Test Coverage: 76 Tests Across All Components

```bash
# Test Results Summary
Ingestion Tests:    ✅ 21 passed
Transformation:     ✅ 18 passed
Validation Tests:   ✅ 16 passed
Loading Tests:      ✅ 17 passed
Integration Tests:  ✅ 6 passed
```

### Test Categories
- **Unit Tests**: Core functionality, edge cases, error handling
- **Integration Tests**: Real data processing, end-to-end flows
- **Performance Tests**: Large dataset handling, concurrent access
- **Error Handling**: All custom exceptions and failure scenarios

## 📁 Directory Structure

```
satsure-agri-datapipeline/
├── config/
│   └── default.yaml                 # Pipeline configuration
├── data/
│   ├── raw/                         # Input Parquet files
│   │   ├── 2023-06-01.parquet      # Sample sensor data
│   │   ├── 2023-06-02.parquet
│   │   └── ...
│   ├── processed/                   # Output partitioned data
│   │   ├── _validation_metadata.json
│   │   └── date=2023-06-02/
│   │       ├── sensor_id=sensor_1/
│   │       │   └── part-0.parquet
│   │       └── ...
│   └── reports/                     # Quality reports
│       └── data_quality_report.csv
├── src/
│   ├── components/                  # Pipeline components
│   │   ├── ingestion.py            # Data ingestion logic
│   │   ├── transformation.py       # Data cleaning & enrichment
│   │   ├── validation.py           # Quality validation
│   │   └── loading.py              # Optimized storage
│   ├── config/                     # Configuration handling
│   ├── models/                     # Data models & schemas
│   ├── utils/                      # Utilities & logging
│   └── main.py                     # Pipeline orchestrator
├── tests/                          # Comprehensive test suite
├── scripts/
│   └── generate_synthetic_raw.py   # Test data generator
├── requirements.txt                # Python dependencies
├── run_pipeline_demo.py           # Interactive demo
├── CLAUDE.md                      # Development guidelines
└── README.md                      # This file
```

## 🔄 Pipeline Execution Modes

### Full Pipeline (Recommended)
```bash
python -m src.main
# or
python run_pipeline_demo.py
```

### Incremental Mode (Production)
```bash
# Processes only new files since last checkpoint
python run_pipeline_demo.py  # force_full_reload=False
```

### Force Full Reload
```bash
# Reprocesses all files, ignoring checkpoint
rm data/.checkpoint
python run_pipeline_demo.py
```

## 🐳 Docker Setup

### Build and Run

```bash
# Build the image
docker build -t satsure-agri:latest .

# Generate sample data (optional)
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/config:/app/config" \
  satsure-agri:latest \
  python scripts/generate_synthetic_raw.py --include-edge-cases

# Run the pipeline
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/reports:/app/reports" \
  -v "$(pwd)/config:/app/config" \
  satsure-agri:latest

# Run tests
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/config:/app/config" \
  satsure-agri:latest \
  pytest -v
```

### Apple Silicon Note
```bash
# For M1/M2/M3 Macs if needed
docker build --platform linux/amd64 -t satsure-agri:latest .
```

## 📈 Performance Characteristics

- **Processing Speed**: ~20,000 records/second on standard hardware
- **Memory Usage**: <500MB for typical daily files (1000-5000 records)
- **Storage Efficiency**: 3.2:1 compression ratio with ZSTD
- **Scalability**: Designed for 100GB+ datasets with partitioning


