import os
import yaml
import duckdb
import pandas as pd
from pathlib import Path

# Set pandas display options to show all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

CONFIG_PATH = Path("/Users/vyomthakkar/Downloads/agri-data-pipeline/config/default.yml")
with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)

RAW_DIR = Path(cfg["paths"]["data_raw"])  # absolute path
print("Raw dir:", RAW_DIR)

# List candidate parquet files
files = sorted(RAW_DIR.glob("*.parquet"))
files[:5], len(files)

# Pick one sample file (first one)
sample = files[0] if files else None
print("Sample file:", sample)
assert sample and sample.exists(), "No parquet files found in data/raw. Add at least one to proceed."


# Connect DuckDB
con = duckdb.connect(database=':memory:')

# Inspect schema and observe the columns
print(con.execute(f"PRAGMA show_tables;").fetchall())
print(con.execute(f"SELECT * FROM parquet_schema('{sample.as_posix()}')").fetchdf())

# Load a small sample and basic counts
con.execute(f"""
    CREATE OR REPLACE VIEW v_raw AS
    SELECT * FROM read_parquet('{sample.as_posix()}')
""")

# Row count
print(con.execute("SELECT COUNT(*) AS row_count FROM v_raw").df())

#Column names, types and description
print(con.execute("DESCRIBE v_raw").df())

# Peek at data
print(con.execute("SELECT * FROM v_raw LIMIT 10").df())


# Nulls and basic stats by column
print(con.execute(
    """
    SELECT
      COUNT(*) AS total_rows,
      SUM(CASE WHEN sensor_id IS NULL THEN 1 ELSE 0 END) AS null_sensor_id,
      SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp,
      SUM(CASE WHEN reading_type IS NULL THEN 1 ELSE 0 END) AS null_reading_type,
      SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_value,
      SUM(CASE WHEN battery_level IS NULL THEN 1 ELSE 0 END) AS null_battery_level
    FROM v_raw
    """
).df())

print(con.execute(
    """
    SELECT reading_type,
           COUNT(*) AS n,
           MIN(value) AS min_value,
           MAX(value) AS max_value,
           AVG(value) AS avg_value
    FROM v_raw
    GROUP BY 1
    ORDER BY 1
    """
).df())


# See sensor distribution
print(con.execute("SELECT sensor_id, reading_type, COUNT(*) FROM v_raw GROUP BY 1,2 ORDER BY 1,2").df())

# Time-based patterns
print(con.execute("SELECT DATE(timestamp) as date, reading_type, AVG(value) FROM v_raw GROUP BY 1,2 ORDER BY 1,2").df())

# Battery levels by sensor - detailed stats
print(con.execute("""
    SELECT sensor_id, 
           COUNT(*) as readings_count,
           MIN(battery_level) as min_battery,
           MAX(battery_level) as max_battery,
           AVG(battery_level) as avg_battery,
           ROUND(AVG(battery_level), 2) as avg_battery_rounded
    FROM v_raw 
    GROUP BY 1 
    ORDER BY 1
""").df())

# Duplicates and timestamp span
print(con.execute(
    """
    WITH cte AS (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY sensor_id, timestamp, reading_type
               ORDER BY sensor_id
             ) AS rn
      FROM v_raw
    )
    SELECT SUM(CASE WHEN rn > 1 THEN 1 ELSE 0 END) AS duplicate_rows,
           MIN(timestamp) AS min_ts,
           MAX(timestamp) AS max_ts
    FROM cte
    """
).df())


# All timestamps in the dataset
# print("All timestamps:")
# print(con.execute("""
#     SELECT timestamp, sensor_id, reading_type
#     FROM v_raw 
#     ORDER BY timestamp
# """).df())

# Hourly reading frequency analysis
print("\nHourly reading patterns:")
print(con.execute("""
    SELECT HOUR(timestamp) as hour,
           COUNT(*) as total_readings,
           COUNT(DISTINCT sensor_id) as active_sensors,
           AVG(CASE WHEN reading_type = 'temperature' THEN value END) as avg_temp,
           AVG(CASE WHEN reading_type = 'humidity' THEN value END) as avg_humidity
    FROM v_raw 
    GROUP BY 1 
    ORDER BY 1
""").df())











