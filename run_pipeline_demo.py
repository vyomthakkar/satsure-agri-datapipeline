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
from src.components.validation import AgricultureValidationComponent
from src.components.loading import AgricultureLoadingComponent


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
        validation = AgricultureValidationComponent(config)
        loading = AgricultureLoadingComponent(config)
        print("✓ Initialized all 4 pipeline components: ingestion, transformation, validation, and loading")

        print("\n📥 Step 1: Data Ingestion")
        print("-" * 30)

        # Run ingestion in incremental mode (respects checkpoint)
        raw_data = ingestion.execute(force_full_reload=False)
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

                print("\n🔍 Step 3: Data Validation")
                print("-" * 30)

                # Run validation
                validation_result = validation.execute(transformed_data)
                print(f"✓ Validation completed: {'PASSED' if validation_result.passed else 'FAILED'}")

                if validation_result.total_records > 0:
                    print(f"   - Records validated: {validation_result.total_records}")
                    print(f"   - Issues found: {len(validation_result.issues_found)}")

                    if validation_result.issues_found:
                        print("   - Sample issues:")
                        for issue in validation_result.issues_found[:3]:
                            print(f"     • {issue}")

                    # Show key quality metrics
                    quality_metrics = validation_result.quality_metrics
                    if "overall_statistics" in quality_metrics:
                        overall = quality_metrics["overall_statistics"]
                        print(f"   - Data quality score: {validation._calculate_quality_score(quality_metrics):.1f}%")
                        print(f"   - Unique sensors: {overall.get('unique_sensors', 0)}")

                    # Check if quality report was generated
                    report_path = Path(config.paths.reports_dir) / "data_quality_report.csv"
                    if report_path.exists():
                        print(f"   - Quality report saved: {report_path}")

                print("\n🔍 Validation Statistics:")
                print("-" * 30)
                for key, value in validation.stats.items():
                    print(f"   {key.replace('_', ' ').title()}: {value}")

                print("\n💾 Step 4: Data Loading")
                print("-" * 30)

                # Run loading
                loading_success = loading.execute(transformed_data, validation_result)
                print(f"✓ Loading completed: {'SUCCESS' if loading_success else 'FAILED'}")

                if loading_success:
                    print(f"   - Records stored: {loading.stats['records_stored']}")
                    print(f"   - Storage size: {loading.stats['storage_size_bytes'] / 1024:.1f} KB")
                    print(f"   - Files written: {loading.stats['files_written']}")
                    print(f"   - Partitions created: {loading.stats['partitions_created']}")

                    if loading.stats['quality_failed_stored'] > 0:
                        print(f"   - Quality failed records stored: {loading.stats['quality_failed_stored']}")

                    # Show storage structure
                    print(f"   - Output location: {config.paths.data_processed}")
                    print(f"   - Compression: {config.write.compression}")
                    print(f"   - Partitioned by: {', '.join(config.write.partition_by)}")

                    # Get storage summary
                    storage_summary = loading.get_storage_summary()
                    if "partitions" in storage_summary:
                        print(f"   - Partition structure:")
                        for partition in storage_summary["partitions"][:3]:  # Show first 3 partitions
                            print(f"     • {partition['date']}: {len(partition['sensors'])} sensors")

                print("\n💾 Loading Statistics:")
                print("-" * 30)
                for key, value in loading.stats.items():
                    if key == "storage_size_bytes":
                        print(f"   {key.replace('_', ' ').title()}: {value / 1024:.1f} KB")
                    else:
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