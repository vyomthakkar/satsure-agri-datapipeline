#!/usr/bin/env python3
"""
Synthetic data generator for agricultural sensor pipeline.

- Creates at least 3 valid Parquet files in data/raw/ with realistic values.
- Injects edge cases: anomalies, duplicates, missing values, and optional schema issues
  (missing columns, extra columns, wrong types) to test pipeline robustness.

Usage:
  python scripts/generate_synthetic_raw.py \
    --dates 2023-06-02 2023-06-03 2023-06-04 \
    --rows-per-file 24 \
    --include-edge-cases

If no args are provided, defaults are used: dates=[2023-06-02, 2023-06-03, 2023-06-04], rows-per-file=24.
"""

from __future__ import annotations

import argparse
import random
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import yaml


@dataclass
class Ranges:
    temperature_min: float
    temperature_max: float
    humidity_min: float
    humidity_max: float
    battery_min: float
    battery_max: float


def load_config(project_root: Path) -> Tuple[Path, Ranges]:
    """Load config/default.yaml and return data_raw path and value ranges."""
    cfg_path = project_root / "config/default.yaml"
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    # Resolve data_raw relative to project root if needed
    data_raw = cfg["paths"]["data_raw"]
    data_raw_path = Path(data_raw)
    if not data_raw_path.is_absolute():
        data_raw_path = (project_root / data_raw).resolve()

    rng = cfg.get("ranges", {})
    r = Ranges(
        temperature_min=float(rng.get("temperature", {}).get("min", -10)),
        temperature_max=float(rng.get("temperature", {}).get("max", 60)),
        humidity_min=float(rng.get("humidity", {}).get("min", 0)),
        humidity_max=float(rng.get("humidity", {}).get("max", 100)),
        battery_min=float(rng.get("battery_level", {}).get("min", 0)),
        battery_max=float(rng.get("battery_level", {}).get("max", 100)),
    )
    return data_raw_path, r


def synth_valid_df(date_str: str, rows: int, rng: Ranges, seed: int = 42) -> pd.DataFrame:
    """Create a valid DataFrame with expected schema and realistic + anomalous values."""
    assert rows <= 30, "Rows per file should be <= 30 as requested"

    random.seed(seed)
    np.random.seed(seed)

    sensors = [f"sensor_{i}" for i in range(1, 6)]  # sensor_1..sensor_5
    reading_types = ["temperature", "humidity"]

    # Create a roughly hourly spread across the day
    start = pd.Timestamp(f"{date_str} 00:00:00")
    hours = np.random.choice(range(24), size=rows, replace=True)
    minutes = np.random.choice([0, 15, 30, 45], size=rows, replace=True)
    timestamps = [start + pd.Timedelta(hours=int(h), minutes=int(m)) for h, m in zip(hours, minutes)]

    # Assign sensors and reading types (slightly favor sensor_5 to mimic findings)
    sensor_probs = [0.15, 0.2, 0.1, 0.2, 0.35]
    sensor_ids = list(np.random.choice(sensors, size=rows, p=sensor_probs))
    rtypes = list(np.random.choice(reading_types, size=rows))

    values = []
    battery = []

    for rt in rtypes:
        if rt == "temperature":
            # Center around 24C with SD ~6
            v = np.random.normal(loc=24, scale=6)
            # 10% chance of out-of-range anomaly
            if np.random.rand() < 0.10:
                v = np.random.choice([rng.temperature_max + 20, rng.temperature_min - 15])
            # Clip to a bit beyond range to keep realistic but allow anomalies
            values.append(float(v))
        else:
            # humidity centered ~50 with SD ~15
            v = np.random.normal(loc=50, scale=15)
            if np.random.rand() < 0.10:
                v = np.random.choice([rng.humidity_max + 30, rng.humidity_min - 10])
            values.append(float(v))

    for _ in range(rows):
        # Typical battery 25-100
        b = float(np.random.uniform(max(25, rng.battery_min), rng.battery_max))
        # 10% missing battery
        if np.random.rand() < 0.10:
            b = np.nan
        # 5% invalid battery outside [0,100]
        if np.random.rand() < 0.05:
            b = np.random.choice([rng.battery_max + 25, rng.battery_min - 10])
        battery.append(b)

    df = pd.DataFrame({
        "sensor_id": sensor_ids,
        "timestamp": timestamps,
        "reading_type": rtypes,
        "value": values,
        "battery_level": battery,
    })

    # Introduce a couple of duplicate rows to test duplicate removal
    if rows >= 10:
        dup_indices = np.random.choice(df.index, size=min(2, rows // 10), replace=False)
        df = pd.concat([df, df.loc[dup_indices]], ignore_index=True)

    # Ensure correct column order as expected by ingestion
    df = df[["sensor_id", "timestamp", "reading_type", "value", "battery_level"]]
    return df


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    print(f"✓ Wrote {len(df)} rows to {path}")


def synth_missing_columns_df(date_str: str, rows: int, rng: Ranges, seed: int = 43) -> pd.DataFrame:
    """Create DataFrame missing 'battery_level' column."""
    base = synth_valid_df(date_str, rows, rng, seed)
    base = base.drop(columns=["battery_level"])
    return base


def synth_extra_columns_df(date_str: str, rows: int, rng: Ranges, seed: int = 44) -> pd.DataFrame:
    """Create DataFrame with an extra 'location' column."""
    base = synth_valid_df(date_str, rows, rng, seed)
    base["location"] = np.random.choice(["field_a", "field_b", "greenhouse"], size=len(base))
    return base


def synth_wrong_types_df(date_str: str, rows: int, rng: Ranges, seed: int = 45) -> pd.DataFrame:
    """Create DataFrame where 'value' column is string-typed to trigger type mismatch."""
    base = synth_valid_df(date_str, rows, rng, seed)
    base["value"] = base["value"].astype(str)
    return base


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic raw Parquet files with edge cases")
    parser.add_argument("--dates", nargs="*", default=["2023-06-02", "2023-06-03", "2023-06-04"],
                        help="List of YYYY-MM-DD dates for valid files")
    parser.add_argument("--rows-per-file", type=int, default=24, help="Rows per valid file (<=30)")
    parser.add_argument("--include-edge-cases", action="store_true", help="Also write invalid schema edge-case files")
    args = parser.parse_args()

    # Resolve project root as repo root (parent of scripts/)
    project_root = Path(__file__).resolve().parent.parent
    data_raw_path, ranges = load_config(project_root)

    # Generate valid files
    for i, date_str in enumerate(args.dates):
        df = synth_valid_df(date_str, rows=min(args.rows_per_file, 30), rng=ranges, seed=42 + i)
        out_path = data_raw_path / f"{date_str}.parquet"
        write_parquet(df, out_path)

    # Generate edge-case files
    if args.include_edge_cases:
        ec_specs = [
            (synth_missing_columns_df, "_missing_columns"),
            (synth_extra_columns_df, "_extra_columns"),
            (synth_wrong_types_df, "_wrong_types"),
        ]
        # Use subsequent dates to avoid clashing with valid files
        # If last provided date is D, start at D+1, D+2, D+3
        last_date = pd.to_datetime(args.dates[-1]) if args.dates else pd.to_datetime("2023-06-04")
        for j, (fn, suffix) in enumerate(ec_specs, start=1):
            date_str = (last_date + pd.Timedelta(days=j)).strftime("%Y-%m-%d")
            df_ec = fn(date_str, rows=min(max(8, args.rows_per_file // 2), 30), rng=ranges, seed=101 + j)
            out_path = data_raw_path / f"{date_str}{suffix}.parquet"
            write_parquet(df_ec, out_path)

    print("\nAll synthetic files generated in:", data_raw_path)


if __name__ == "__main__":
    main()
