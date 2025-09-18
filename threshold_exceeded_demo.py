#!/usr/bin/env python3
"""
Threshold Exceeded Demo for Agricultural Sensor Pipeline

This script demonstrates what happens when validation thresholds are exceeded.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timezone

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config.models import PipelineConfig
from src.components.validation import AgricultureValidationComponent


def demo_threshold_exceeding():
    """Demonstrate what happens when validation thresholds are exceeded."""

    print("🚨 Agricultural Sensor Pipeline - Threshold Exceeded Demo")
    print("=" * 60)

    # Load standard config
    config = PipelineConfig.from_yaml(Path("config/default.yaml"))

    print("📋 Current Validation Thresholds:")
    print(f"  max_missing_percentage: {config.validation.max_missing_percentage}%")
    print(f"  max_anomaly_percentage: {config.validation.max_anomaly_percentage}%")
    print(f"  max_gap_hours: {config.validation.max_gap_hours} hours")

    scenarios = [
        {
            "name": "🔴 High Missing Data Scenario",
            "description": "50% missing values (exceeds 20% threshold)",
            "data_issues": "50% missing values"
        },
        {
            "name": "🟠 High Anomaly Scenario",
            "description": "25% anomalous readings (exceeds 10% threshold)",
            "data_issues": "25% anomalous readings"
        },
        {
            "name": "🟡 Combined Issues Scenario",
            "description": "Multiple thresholds exceeded simultaneously",
            "data_issues": "30% missing + 15% anomalous"
        }
    ]

    for i, scenario in enumerate(scenarios, 1):
        print(f"\n{scenario['name']}")
        print("-" * 50)
        print(f"Scenario: {scenario['description']}")
        print(f"Data Issues: {scenario['data_issues']}")

        # Create problematic data based on scenario
        if "Missing Data" in scenario['name']:
            data = create_high_missing_data()
        elif "High Anomaly" in scenario['name']:
            data = create_high_anomaly_data()
        else:  # Combined
            data = create_combined_issues_data()

        print(f"\n📊 Data Overview:")
        print(f"  Total records: {len(data)}")
        missing_count = data['value'].isnull().sum()
        anomaly_count = data['anomalous_reading'].sum()
        print(f"  Missing values: {missing_count} ({missing_count/len(data)*100:.1f}%)")
        print(f"  Anomalous readings: {anomaly_count} ({anomaly_count/len(data)*100:.1f}%)")

        # Run validation
        print(f"\n🔍 Running Validation...")
        validation_component = AgricultureValidationComponent(config)

        try:
            result = validation_component.execute(data)

            print(f"\n📋 Validation Results:")
            print(f"  Overall Status: {'✅ PASSED' if result.passed else '❌ FAILED'}")
            print(f"  Records Validated: {result.total_records}")
            print(f"  Issues Found: {len(result.issues_found)}")

            if result.issues_found:
                print(f"\n🚨 Specific Issues:")
                for issue in result.issues_found[:5]:  # Show first 5 issues
                    print(f"    • {issue}")
                if len(result.issues_found) > 5:
                    print(f"    ... and {len(result.issues_found) - 5} more issues")

            # Show quality metrics
            quality_metrics = result.quality_metrics
            if "missing_values_by_type" in quality_metrics:
                print(f"\n📊 Missing Data Analysis:")
                for reading_type, metrics in quality_metrics["missing_values_by_type"].items():
                    status = "🔴 EXCEEDED" if metrics["missing_percentage"] > config.validation.max_missing_percentage else "✅ OK"
                    print(f"    {reading_type}: {metrics['missing_percentage']:.1f}% missing {status}")

            if "anomalies_by_type" in quality_metrics:
                print(f"\n⚠️  Anomaly Analysis:")
                for reading_type, metrics in quality_metrics["anomalies_by_type"].items():
                    status = "🔴 EXCEEDED" if metrics["anomaly_percentage"] > config.validation.max_anomaly_percentage else "✅ OK"
                    print(f"    {reading_type}: {metrics['anomaly_percentage']:.1f}% anomalous {status}")

            # Show what this means for the pipeline
            print(f"\n🔄 Pipeline Impact:")
            if result.passed:
                print("    ✅ Data proceeds to loading component")
                print("    ✅ Pipeline continues normally")
                print("    ✅ Data gets stored in processed directory")
            else:
                print("    ❌ Validation failure flagged")
                print("    ⚠️  Data quality issues logged")
                print("    📊 Quality report generated with failures")
                print("    🔄 Pipeline continues BUT flags quality concerns")
                print("    📧 Operations team should be notified")

            print(f"\n💡 What Happens Next:")
            print("    1. Quality report CSV generated with threshold violations")
            print("    2. Warning logs written for operations team review")
            print("    3. Data still proceeds to loading (with quality flags)")
            print("    4. Downstream consumers can check validation.passed flag")
            print("    5. Alerting systems can trigger based on validation.passed=False")

        except Exception as e:
            print(f"❌ Validation failed with error: {e}")

        if i < len(scenarios):
            print(f"\n" + "="*60)


def create_high_missing_data():
    """Create data with 50% missing values."""
    data = pd.DataFrame({
        'sensor_id': ['sensor_1'] * 10,
        'timestamp': [datetime(2023, 6, 1, 10, i, tzinfo=timezone.utc) for i in range(10)],
        'reading_type': ['temperature'] * 10,
        'value': [25.0, np.nan, 26.0, np.nan, 27.0, np.nan, 28.0, np.nan, 24.0, np.nan],  # 50% missing
        'battery_level': [90.0, 85.0, 80.0, 75.0, 70.0, 65.0, 60.0, 55.0, 50.0, 45.0],
        'date': [datetime(2023, 6, 1).date()] * 10,
        'daily_avg_value': [26.0] * 10,
        'rolling_avg_value': [25.0] * 10,
        'anomalous_reading': [False] * 10
    })
    return data


def create_high_anomaly_data():
    """Create data with 25% anomalous readings."""
    data = pd.DataFrame({
        'sensor_id': ['sensor_1'] * 12,
        'timestamp': [datetime(2023, 6, 1, 10, i, tzinfo=timezone.utc) for i in range(12)],
        'reading_type': ['temperature'] * 12,
        'value': [25.0, 26.0, 27.0, 28.0, 24.0, 26.0, 25.0, 27.0, 26.0, 25.0, 28.0, 24.0],
        'battery_level': [90.0] * 12,
        'date': [datetime(2023, 6, 1).date()] * 12,
        'daily_avg_value': [26.0] * 12,
        'rolling_avg_value': [25.0] * 12,
        'anomalous_reading': [False, False, False, True, False, False, False, True, False, True, False, False]  # 25% anomalous
    })
    return data


def create_combined_issues_data():
    """Create data with both missing values and anomalies."""
    data = pd.DataFrame({
        'sensor_id': ['sensor_1'] * 10,
        'timestamp': [datetime(2023, 6, 1, 10, i, tzinfo=timezone.utc) for i in range(10)],
        'reading_type': ['temperature'] * 10,
        'value': [25.0, np.nan, 26.0, np.nan, 27.0, np.nan, 28.0, 24.0, 26.0, 25.0],  # 30% missing
        'battery_level': [90.0] * 10,
        'date': [datetime(2023, 6, 1).date()] * 10,
        'daily_avg_value': [26.0] * 10,
        'rolling_avg_value': [25.0] * 10,
        'anomalous_reading': [False, False, True, False, False, False, True, False, False, False]  # 20% anomalous (15% of non-null)
    })
    return data


if __name__ == "__main__":
    demo_threshold_exceeding()