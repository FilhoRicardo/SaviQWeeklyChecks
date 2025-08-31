# Dexcell API Data Extraction and Analysis Toolkit

This is a comprehensive Python toolkit for extracting, analyzing, and reporting energy consumption data from the Dexcell IoT platform. The toolkit provides automated device configuration generation, data extraction via API, and multiple analysis workflows including data quality checks, trend analysis, and out-of-hours consumption monitoring.

## Project Structure

```
client_configs/         # JSON configuration files for data extraction
outputs/                # CSV outputs and report files

config_generator.py     # Generates device configuration JSONs from Dexcell API

dicecell_extractor.py    # Extracts data from Dexcell API concurrently with retries

data_quality_check.py   # Hourly data quality analysis tool

trend_check.py          # Trend analysis tool for weekly consumption comparisons

working_hours_check.py   # Out-of-hours energy consumption analysis tool

WeeklyCheck.py          # Orchestrates full hourly data analysis workflow

DataExtractor.py        # Example code snippet showing how to use extractor
```

## Features

- Fetches all relevant devices and parameters from Dexcell API
- Generates client-specific JSON config files for data extraction
- Extracts consumption data concurrently with error handling and retries
- Analyzes hourly data quality including completeness, zeros, and negatives
- Detects trends by comparing consecutive weekly periods
- Flags out-of-hours energy consumption for operational review
- Generates detailed CSV and human-readable text reports for all analyses

## Installation

1. Ensure Python 3.7 or above is installed
2. Install required packages:

```bash
pip install requests pandas python-dateutil numpy
```

## Usage

### 1. Generate Device Configurations

Run `config_generator.py` to fetch devices from API and generate configuration files.

### 2. Extract Data

Use `dexcell_extractor.py` (via `DexcellDataExtractor` class) with generated config files to extract data.

Example use:
```python
from dexcell_extractor import DexcellDataExtractor
extractor = DexcellDataExtractor('client_config.json', debug=True)
data = extractor.extract_data()
extractor.save_to_csv('output_data.csv')
```

### 3. Run Analyses

All analyses expect configuration JSON and extracted data CSV in `client_configs` and `outputs` respectively.

- **Data Quality Analysis:**
```python
from data_quality_check import HourlyDataQualityAnalyzer
analyzer = HourlyDataQualityAnalyzer('config.json', 'data.csv')
quality_df = analyzer.analyze_quality()
analyzer.save_report('quality_report.csv')
analyzer.save_text_report('quality_summary.txt')
```

- **Trend Analysis:**
```python
from trend_check import DataTrendAnalyzer
trend_analyzer = DataTrendAnalyzer('config.json', 'data.csv', trend_threshold=10.0)
trend_df = trend_analyzer.analyze_trends()
trend_analyzer.save_report('trend_report.csv')
```

- **Out-of-Hours Consumption Analysis:**
```python
from working_hours_check import OutOfHoursConsumptionAnalyzer
out_hours_analyzer = OutOfHoursConsumptionAnalyzer('config.json', 'data.csv', out_of_hours_threshold=30.0)
out_hours_df = out_hours_analyzer.analyze_consumption()
out_hours_analyzer.save_report('out_of_hours_report.csv')
```

### 4. Full Workflow

Run `WeeklyCheck.py` to execute data extraction and all analyses sequentially with configured files.

## Configuration

Each client config JSON includes:
- API tokens and client names
- List of parameters to extract
- Request type (`hourly` or `monthly`)
- Date range (`start_date` and `end_date`)
- List of devices with IDs, names, and parameters

Example config snippet:
```json
{
  "api_keys": [{"token": "token", "client_name": "Client"}],
  "params": ["EACTIVE", "POWER"],
  "request_type": "hourly",
  "start_date": "2024-01-01T00:00:00Z",
  "end_date": "2024-01-15T23:59:59Z",
  "devices": [{"device_id": 12345, "name": "Device1", "param": "EACTIVE"}]
}
```

## Reports

- CSV files with detailed device-level metrics
- Text files with executive summaries and flagged issues

## Dependencies

- requests
- pandas
- python-dateutil
- numpy

## Support

For API token issues or data questions, consult the Dexcell API official docs or your support contact.

---

This toolkit is intended for internal use with the Dexcell API for energy consumption insights.
