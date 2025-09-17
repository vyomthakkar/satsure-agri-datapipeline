# Findings from Data Exploration

## Raw Data used:
data/raw/2023-06-01.parquet ([https://drive.google.com/file/d/1JzvmQU1ETr4MBgOzTYbpjEm43QVOwXIW/view?usp=sharing](https://drive.google.com/file/d/1JzvmQU1ETr4MBgOzTYbpjEm43QVOwXIW/view?usp=sharing)) 

## Shape of data:
30 Rows, 5 columns
Each row consists of a sensor reading with the following 5 columns:
- `sensor_id`
- `timestamp` 
- `reading_type`
- `value`
- `battery_level`

## Data Quality:
1. No null/missing values in any column. All rows have the 5 columns populated.
2. No duplicate records, and each row is unique.

## Data Context:

### Time Coverage:
- **Start:** June 5, 2025 at 11:23:48 AM
- **End:** June 5, 2025 at 1:48:48 PM
- **Duration:** Approximately 2.5 hours of sensor data

## Breakdown of sensor data:
- **Humidity readings:** 13
- **Temperature readings:** 17
- **Total readings:** 30 (matches the total number of rows in the dataset)

## Sensor data stats:

| reading_type | n  | min_value | max_value | avg_value |
|--------------|----|-----------|-----------|-----------|
| humidity     | 13 | 12.12     | 33.29     | 24.58     |
| temperature  | 17 | 11.24     | 32.49     | 22.27     |

## Sensor IDs:
sensor_1, sensor_2, sensor_3, sensor_4, sensor_5

## Breakdown of battery levels stats of each sensor:

| sensor_id | readings_count | min_battery | max_battery | avg_battery | avg_battery_rounded |
|-----------|----------------|-------------|-------------|-------------|---------------------|
| sensor_1  | 5              | 25.34       | 98.73       | 56.05       | 56.05               |
| sensor_2  | 6              | 29.62       | 94.81       | 60.76       | 60.76               |
| sensor_3  | 2              | 43.47       | 53.11       | 48.29       | 48.29               |
| sensor_4  | 7              | 26.97       | 82.70       | 62.84       | 62.84               |
| sensor_5  | 10             | 27.00       | 93.70       | 63.01       | 63.01               |

## Breakdown of distribution of data captured by each sensor:

| sensor_id | reading_type | count |
|-----------|--------------|-------|
| sensor_1  | humidity     | 3     |
| sensor_1  | temperature  | 2     |
| sensor_2  | humidity     | 3     |
| sensor_2  | temperature  | 3     |
| sensor_3  | humidity     | 2     |
| sensor_4  | humidity     | 2     |
| sensor_4  | temperature  | 5     |
| sensor_5  | humidity     | 3     |
| sensor_5  | temperature  | 7     |

Note that, 10 out 30 readings are from one sensor: sensor_5


## Hourly reading patterns:

| hour | total_readings | active_sensors | avg_temp | avg_humidity |
|------|----------------|----------------|----------|---------------|
| 11   | 8              | 4              | 24.52    | 18.09         |
| 12   | 12             | 5              | 21.08    | 25.46         |
| 13   | 10             | 5              | 21.02    | 26.28         |





