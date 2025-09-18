#!/usr/bin/env python3
"""
Demo script to run ingestion and transformation on real data.

This shows how our implemented components work with the actual raw data.
"""

import sys
from pathlib import Path

# Add src to path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.config.models import PipelineConfig
from src.components.ingestion import ParquetIngestionComponent
from src.components.transformation import AgricultureTransformationComponent


def main():
    """Run ingestion and transformation on the real data."""
    print("🌾 Agricultural Sensor Data Pipeline Demo")
    print("=" * 50)

    try:
        # Load configuration
        config_path = Path("config/default.yaml")
        config = PipelineConfig.from_yaml(config_path)
        print(f"✓ Loaded configuration from {config_path}")

        # Initialize components
        ingestion = ParquetIngestionComponent(config)
        transformation = AgricultureTransformationComponent(config)
        print("✓ Initialized ingestion and transformation components")

        print("\n📥 Step 1: Data Ingestion")
        print("-" * 30)

        # Run ingestion with force_full_reload to reprocess all files
        raw_data = ingestion.execute(force_full_reload=True)
        print(f"✓ Ingested {len(raw_data)} records")

        if len(raw_data) > 0:
            print(f"   - Data shape: {raw_data.shape}")
            print(f"   - Columns: {list(raw_data.columns)}")
            print(f"   - Date range: {raw_data['timestamp'].min()} to {raw_data['timestamp'].max()}")
            print(f"   - Sensors: {sorted(raw_data['sensor_id'].unique())}")
            print(f"   - Reading types: {sorted(raw_data['reading_type'].unique())}")

            print("\n📊 Sample of raw data:")
            print(raw_data.head(3).to_string())

            print("\n🔄 Step 2: Data Transformation")
            print("-" * 30)

            # Run transformation
            transformed_data = transformation.execute(raw_data)
            print(f"✓ Transformed {len(transformed_data)} records")

            if len(transformed_data) > 0:
                print(f"   - Output shape: {transformed_data.shape}")
                print(f"   - New columns added: {set(transformed_data.columns) - set(raw_data.columns)}")

                # Show anomaly detection results
                anomalies = transformed_data[transformed_data['anomalous_reading'] == True]
                print(f"   - Anomalies detected: {len(anomalies)}")

                if len(anomalies) > 0:
                    print(f"   - Anomalous values: {sorted(anomalies['value'].unique())}")

                print("\n📊 Sample of transformed data:")
                # Show a few columns of interest
                sample_cols = ['sensor_id', 'timestamp', 'reading_type', 'value',
                             'daily_avg_value', 'rolling_avg_value', 'anomalous_reading']
                available_cols = [col for col in sample_cols if col in transformed_data.columns]
                print(transformed_data[available_cols].head(3).to_string())

                print("\n📊 Data Analysis:")
                print("-" * 30)

                # Value ranges by reading type
                for reading_type in transformed_data['reading_type'].unique():
                    subset = transformed_data[transformed_data['reading_type'] == reading_type]
                    print(f"   {reading_type.title()}:")
                    print(f"     - Range: {subset['value'].min():.2f} to {subset['value'].max():.2f}")
                    print(f"     - Average: {subset['value'].mean():.2f}")
                    print(f"     - Records: {len(subset)}")

                # Battery analysis
                print(f"   Battery Levels:")
                print(f"     - Range: {transformed_data['battery_level'].min():.1f}% to {transformed_data['battery_level'].max():.1f}%")
                print(f"     - Average: {transformed_data['battery_level'].mean():.1f}%")

                # Show timezone conversion worked
                sample_time = transformed_data['timestamp'].iloc[0]
                print(f"   Timezone: {sample_time.tzinfo} (UTC+05:30 conversion applied)")

                print("\n📈 Transformation Statistics:")
                print("-" * 30)
                for key, value in transformation.stats.items():
                    print(f"   {key.replace('_', ' ').title()}: {value}")

            else:
                print("⚠️  No data after transformation")

        else:
            print("⚠️  No data ingested")

        # Save transformed data for inspection if we got any
        if 'transformed_data' in locals() and len(transformed_data) > 0:
            output_path = Path("data/demo_transformed_output.parquet")
            transformed_data.to_parquet(output_path)
            print(f"\n💾 Saved transformed data to: {output_path}")

        print("\n✅ Pipeline demo completed successfully!")

    except Exception as e:
        print(f"\n❌ Pipeline demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()