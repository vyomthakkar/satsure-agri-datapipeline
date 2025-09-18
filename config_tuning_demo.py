#!/usr/bin/env python3
"""
Configuration Tuning Demo for Agricultural Sensor Pipeline

This script demonstrates how users can tune validation thresholds
for different agricultural monitoring scenarios.
"""

import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config.models import PipelineConfig
from src.components.validation import AgricultureValidationComponent
import pandas as pd
from datetime import datetime, timezone


def demo_config_scenarios():
    """Demonstrate different validation configurations for various agricultural scenarios."""

    print("🌾 Agricultural Sensor Pipeline - Configuration Tuning Demo")
    print("=" * 60)

    # Create sample data for testing
    sample_data = pd.DataFrame({
        'sensor_id': ['sensor_1'] * 5,
        'timestamp': [datetime(2023, 6, 1, 10, i, tzinfo=timezone.utc) for i in range(5)],
        'reading_type': ['temperature'] * 5,
        'value': [25.0, 26.0, 27.0, 28.0, 24.0],
        'battery_level': [90.0, 85.0, 80.0, 75.0, 70.0],
        'date': [datetime(2023, 6, 1).date()] * 5,
        'daily_avg_value': [26.0] * 5,
        'rolling_avg_value': [25.0, 25.5, 26.0, 26.5, 26.0],
        'anomalous_reading': [False] * 5
    })

    scenarios = [
        {
            "name": "🏡 Greenhouse Monitoring (High Precision)",
            "description": "Indoor controlled environment with reliable connectivity",
            "config": {
                "max_missing_percentage": 5.0,    # Very low tolerance for missing data
                "max_anomaly_percentage": 3.0,     # High sensitivity to anomalies
                "expected_frequency_hours": 1,     # Hourly readings expected
                "max_gap_hours": 1.0               # Alert after 1 hour gap
            }
        },
        {
            "name": "🌾 Field Monitoring (Standard)",
            "description": "Outdoor field sensors with moderate connectivity",
            "config": {
                "max_missing_percentage": 20.0,   # Standard tolerance (default)
                "max_anomaly_percentage": 10.0,   # Standard anomaly threshold
                "expected_frequency_hours": 1,     # Hourly readings expected
                "max_gap_hours": 2.0              # Alert after 2 hour gap
            }
        },
        {
            "name": "🏔️ Remote Monitoring (Resilient)",
            "description": "Remote locations with challenging connectivity",
            "config": {
                "max_missing_percentage": 40.0,   # High tolerance for connectivity issues
                "max_anomaly_percentage": 15.0,   # More lenient anomaly detection
                "expected_frequency_hours": 4,     # 4-hour reading intervals
                "max_gap_hours": 8.0              # Alert after 8 hour gap
            }
        }
    ]

    for scenario in scenarios:
        print(f"\n{scenario['name']}")
        print("-" * 50)
        print(f"Use Case: {scenario['description']}")
        print()

        # Show configuration
        print("📋 Configuration:")
        for key, value in scenario['config'].items():
            print(f"  {key}: {value}")

        # Show impact on thresholds (simulate - for demo purposes)
        config = scenario['config']
        print(f"\n📊 Validation Impact:")
        print(f"  Missing Data Tolerance: Up to {config['max_missing_percentage']}% missing readings")
        print(f"  Anomaly Sensitivity: Alert if >{config['max_anomaly_percentage']}% anomalous readings")
        print(f"  Reading Frequency: Every {config['expected_frequency_hours']} hour(s)")
        print(f"  Gap Alerting: Alert after {config['max_gap_hours']} hour gap")

        print(f"\n🎯 Best For:")
        if "Greenhouse" in scenario['name']:
            print("  • Indoor farms with stable power and WiFi")
            print("  • High-value crops requiring precise monitoring")
            print("  • Research facilities with strict data requirements")
        elif "Field" in scenario['name']:
            print("  • Traditional agricultural fields")
            print("  • Balanced monitoring for most farm operations")
            print("  • Standard commercial agriculture")
        else:  # Remote
            print("  • Distant pastures or remote agricultural areas")
            print("  • Locations with cellular/satellite connectivity")
            print("  • Extensive farming operations")

        print()

    print("\n🔧 How to Configure:")
    print("1. Edit config/default.yaml validation section")
    print("2. Adjust thresholds based on your agricultural scenario")
    print("3. Run pipeline with: python run_pipeline_demo.py")
    print("\nExample config/default.yaml section:")
    print("```yaml")
    print("validation:")
    print("  max_missing_percentage: 20.0      # Adjust for your tolerance")
    print("  max_anomaly_percentage: 10.0      # Adjust for your sensitivity")
    print("  expected_frequency_hours: 1        # Match your sensor schedule")
    print("  max_gap_hours: 2.0                # Set your alerting threshold")
    print("```")


if __name__ == "__main__":
    demo_config_scenarios()