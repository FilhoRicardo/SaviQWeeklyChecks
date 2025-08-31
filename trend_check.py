#!/usr/bin/env python3
"""
Data Trend Analysis Tool - COMPLETE FIXED VERSION

This module provides comprehensive trend analysis for consumption data extracted
from the Dexcell API. It compares two weeks of data by splitting 14-day periods
into consecutive 7-day weeks and analyzing week-over-week variance patterns.

CRITICAL FIX: Uses config dates to determine analysis periods and proper timezone handling.
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from dateutil import parser


class DataTrendAnalyzer:
    """
    Analyzes trending patterns and week-over-week variance in Dexcell extracted data.
    
    This class processes data periods by splitting them into two consecutive
    weeks and comparing the total consumption/usage between periods. It identifies
    significant deviations that may indicate equipment issues, behavioral changes,
    or data quality problems.
    
    The analyzer handles timezone conversions, generates detailed trend reports,
    and flags devices that require attention due to significant variance patterns.
    
    CRITICAL FIX: Uses configuration dates to determine analysis periods.
    
    Attributes:
        config_path (Path): Path to the JSON configuration file
        csv_path (Path): Path to the CSV data file
        output_dir (Path): Directory for saving output reports
        config (Dict): Loaded configuration data
        data (pd.DataFrame): Processed consumption data
        results (List[Dict[str, Any]]): Trend analysis results for all devices
        trend_threshold (float): Percentage threshold for flagging significant trends
        analysis_start_date (datetime): Start date for the analysis period
        analysis_end_date (datetime): End date for the analysis period
        
    Example:
        analyzer = DataTrendAnalyzer(
            config_filename="client_config.json",
            csv_filename="trend_data.csv",
            trend_threshold=10.0
        )
        results_df = analyzer.analyze_trends()
        analyzer.save_report("trend_analysis.csv")
        analyzer.save_text_report("trend_summary.txt")
    """
    
    def __init__(
        self, 
        config_filename: str, 
        csv_filename: str, 
        trend_threshold: float = 10.0
    ) -> None:
        """
        Initialize the trend analyzer with configuration and data files.
        
        Sets up file paths, loads configuration and data, and calculates the analysis
        period based on the configuration file dates.
        
        Args:
            config_filename: Name of JSON config file in 'client_configs' folder
            csv_filename: Name of CSV data file in 'outputs' folder
            trend_threshold: Percentage threshold for flagging significant changes (default: 10.0%)
        
        Raises:
            ValueError: If trend threshold is outside valid range (0-100)
            FileNotFoundError: If config or CSV files cannot be found
            ValueError: If required configuration fields are missing or data format is invalid
        
        Example:
            analyzer = DataTrendAnalyzer("config.json", "trend_data.csv", 15.0)
        """
        # Validate threshold parameter
        if trend_threshold < 0 or trend_threshold > 100:
            raise ValueError("Trend threshold must be between 0 and 100 percent")
            
        # Setup base directory structure for file operations
        base_dir = Path(__file__).parent
        config_dir = base_dir / 'client_configs'
        output_dir = base_dir / 'outputs'
        
        # Ensure required directories exist
        config_dir.mkdir(exist_ok=True)
        output_dir.mkdir(exist_ok=True)
        
        # Construct full file paths
        config_path = config_dir / config_filename
        csv_path = output_dir / csv_filename
        
        # Store paths for later use
        self.config_path: Path = config_path
        self.csv_path: Path = csv_path
        self.output_dir: Path = output_dir
        
        print(f"Loading configuration from: {config_path}")
        print(f"Loading data from: {csv_path}")
        
        # Load and validate configuration and data files
        self.config: Dict = self._load_config(str(config_path))
        self.data: pd.DataFrame = self._load_data(str(csv_path))
        self.results: List[Dict[str, Any]] = []
        self.trend_threshold: float = trend_threshold
        
        # CRITICAL FIX: Use config dates to determine analysis periods
        self.analysis_start_date: datetime = self._parse_config_date(self.config['start_date'])
        self.analysis_end_date: datetime = self._parse_config_date(self.config['end_date'])
        
        # DEBUG: Display analysis period for verification
        print(f"🔍 Trend analysis period:")
        print(f"    Start: {self.analysis_start_date}")
        print(f"    End: {self.analysis_end_date}")
        
        # Validate that we have appropriate data period for trend analysis
        self._validate_data_period()
        
        # Prepare data with additional columns needed for trend analysis
        self._prepare_trend_analysis()
    
    def _load_config(self, config_file_path: str) -> Dict:
        """
        Load and validate the JSON configuration file for trend analysis.
        
        Args:
            config_file_path: Full path to the JSON configuration file
            
        Returns:
            Dict containing the loaded and validated configuration data
            
        Raises:
            FileNotFoundError: If the configuration file cannot be found
            ValueError: If JSON is invalid or required fields are missing
        """
        try:
            with open(config_file_path, 'r', encoding='utf-8') as file:
                config = json.load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
        
        # Validate that all required configuration fields are present
        required_fields = ['devices', 'start_date', 'end_date', 'request_type']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required configuration field: {field}")
        
        return config
    
    def _parse_config_date(self, date_str: str) -> datetime:
        """
        Parse configuration date string to datetime object.
        
        Args:
            date_str: Date string from configuration
            
        Returns:
            datetime: Parsed date as naive datetime
            
        Raises:
            ValueError: If date format is invalid
        """
        try:
            # Handle various date formats including timezone indicators
            clean_date = date_str.replace('Z', '+00:00')
            
            # Parse the date using dateutil parser for flexibility
            parsed_date = parser.isoparse(clean_date)
            
            # Return as naive datetime
            return parsed_date.replace(tzinfo=None)
            
        except ValueError as e:
            raise ValueError(f"Invalid date format in configuration: {date_str} - {e}")
    
    def _load_data(self, csv_file_path: str) -> pd.DataFrame:
        """
        Load and prepare the CSV data for trend analysis.
        
        CRITICAL FIX: Proper timezone handling to prevent time misclassification.
        
        Args:
            csv_file_path: Full path to the CSV data file
            
        Returns:
            pd.DataFrame with processed timestamp data ready for analysis
            
        Raises:
            FileNotFoundError: If the CSV data file cannot be found
            ValueError: If required columns are missing or timestamp format is invalid
        """
        try:
            data = pd.read_csv(csv_file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV data file not found: {csv_file_path}")
        
        # Validate that all required columns exist for trend analysis
        required_columns = ['device_id', 'device_name', 'param_key', 'timestamp', 'value']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in CSV data: {missing_columns}")
        
        # CRITICAL FIX: Proper timezone handling to prevent time misclassification
        try:
            # Parse timestamps with UTC handling for mixed timezone data
            data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)
            
            # CRITICAL FIX: Add offset to handle timezone conversion properly
            # This ensures that timestamps don't shift to wrong time periods
            data['timestamp'] = data['timestamp'] + pd.Timedelta(hours=1)
            
            # Convert to naive datetime after applying offset
            data['timestamp'] = data['timestamp'].dt.tz_convert(None)
            
            # DEBUG: Show sample of processed timestamps
            print(f"🔍 Sample processed timestamps:")
            for i, row in data.head(3).iterrows():
                print(f"    {row['device_name']}: {row['timestamp']}")
            
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format in data: {e}")
        
        return data
    
    def _validate_data_period(self) -> None:
        """
        Validate that the data period is appropriate for trend analysis.
        
        Checks the configuration dates to ensure we have a sufficient period
        for meaningful week-over-week comparison analysis.
        
        Raises:
            ValueError: If the data period is insufficient or frequency is invalid
        """
        # Calculate the total period duration
        period_duration = self.analysis_end_date - self.analysis_start_date
        
        # Require at least 7 days for basic trend analysis
        if period_duration.days < 7:
            raise ValueError(
                f"Data period must be at least 7 days for trend analysis. "
                f"Current period: {period_duration.days} days"
            )
        
        # Warn if not exactly 14 days but allow analysis to proceed
        if abs(period_duration.days - 14) > 1:
            print(f"⚠️  Warning: Data period is {period_duration.days} days. "
                  f"Optimal trend analysis requires 14 days for two-week comparison.")
        
        # Ensure we have hourly data for meaningful trend analysis
        if self.config.get('request_type') != 'hourly':
            print(f"⚠️  Warning: Trend analysis works best with hourly data frequency. "
                  f"Current frequency: {self.config.get('request_type', 'unknown')}")
    
    def _prepare_trend_analysis(self) -> None:
        """
        Prepare data with filtering and sorting for trend analysis.
        
        Filters data to the analysis period and sorts it for proper
        chronological processing during trend calculations.
        """
        # Filter data to analysis period
        timestamp_filter = (
            (self.data['timestamp'] >= self.analysis_start_date) & 
            (self.data['timestamp'] <= self.analysis_end_date)
        )
        
        # DEBUG: Show data volume before and after filtering
        print(f"🔍 Data before filtering: {len(self.data):,} records")
        self.data = self.data[timestamp_filter].copy()
        print(f"🔍 Data after filtering: {len(self.data):,} records")
        
        # Sort data for consistent processing and analysis
        self.data = self.data.sort_values(['device_id', 'param_key', 'timestamp'])
    
    def _split_data_into_weeks(self, device_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Split device data into two consecutive time periods for comparison.
        
        Args:
            device_data: DataFrame containing data for a single device/parameter
            
        Returns:
            Tuple containing (first_period_data, second_period_data) DataFrames
            
        Raises:
            ValueError: If insufficient data is available for splitting
        """
        if device_data.empty:
            raise ValueError("Cannot split empty device data into periods")
        
        # Sort data by timestamp to ensure proper chronological order
        device_data = device_data.sort_values('timestamp')
        
        # Calculate the midpoint timestamp for splitting into two periods
        min_timestamp = device_data['timestamp'].min()
        max_timestamp = device_data['timestamp'].max()
        midpoint_timestamp = min_timestamp + (max_timestamp - min_timestamp) / 2
        
        # Split data into two approximately equal time periods
        first_period_data = device_data[device_data['timestamp'] <= midpoint_timestamp].copy()
        second_period_data = device_data[device_data['timestamp'] > midpoint_timestamp].copy()
        
        return first_period_data, second_period_data
    
    def _calculate_period_totals(self, period_data: pd.DataFrame) -> Dict[str, float]:
        """
        Calculate summary statistics for a time period's worth of data.
        
        Args:
            period_data: DataFrame containing one period of device measurements
            
        Returns:
            Dictionary containing calculated statistics (total, average, count, etc.)
        """
        if period_data.empty:
            return {
                'total': 0.0,
                'average': 0.0,
                'count': 0,
                'min_value': 0.0,
                'max_value': 0.0,
                'zero_count': 0
            }
        
        # Remove any non-numeric or NaN values for accurate calculations
        numeric_values = pd.to_numeric(period_data['value'], errors='coerce').dropna()
        
        return {
            'total': float(numeric_values.sum()),
            'average': float(numeric_values.mean()) if len(numeric_values) > 0 else 0.0,
            'count': len(numeric_values),
            'min_value': float(numeric_values.min()) if len(numeric_values) > 0 else 0.0,
            'max_value': float(numeric_values.max()) if len(numeric_values) > 0 else 0.0,
            'zero_count': int((numeric_values == 0).sum())
        }
    
    def _calculate_trend_variance(self, period1_total: float, period2_total: float) -> Dict[str, Any]:
        """
        Calculate variance metrics between two periods of data.
        
        Args:
            period1_total: Total consumption/usage for the first period
            period2_total: Total consumption/usage for the second period
            
        Returns:
            Dictionary containing variance calculations and trend flags
        """
        # Handle edge case where first period total is zero
        if period1_total == 0:
            if period2_total == 0:
                percentage_change = 0.0
                trend_direction = 'stable'
            else:
                # Cannot calculate percentage change from zero baseline
                percentage_change = float('inf')
                trend_direction = 'significant_increase'
        else:
            # Standard percentage change calculation: ((new - old) / old) * 100
            percentage_change = ((period2_total - period1_total) / period1_total) * 100
            
        # Determine trend direction based on percentage change
        if percentage_change == float('inf'):
            trend_direction = 'significant_increase'
        elif abs(percentage_change) <= self.trend_threshold:
            trend_direction = 'stable'
        elif percentage_change > self.trend_threshold:
            trend_direction = 'increasing'
        else:
            trend_direction = 'decreasing'
        
        # Flag significant trends that exceed the configured threshold
        is_significant_trend = (percentage_change == float('inf') or 
                               abs(percentage_change) > self.trend_threshold)
        
        return {
            'percentage_change': percentage_change if percentage_change != float('inf') else 999.99,
            'absolute_difference': period2_total - period1_total,
            'trend_direction': trend_direction,
            'is_flagged': is_significant_trend,
            'baseline_period': period1_total,
            'comparison_period': period2_total
        }
    
    def _analyze_device_trend(
        self, 
        device_data: pd.DataFrame, 
        device_id: int,
        device_name: str, 
        param_key: str
    ) -> Dict[str, Any]:
        """
        Analyze trend patterns for a single device/parameter combination.
        
        Args:
            device_data: DataFrame containing data for this specific device/parameter
            device_id: Unique identifier for the device
            device_name: Human-readable name of the device
            param_key: Parameter being analyzed (e.g., 'EACTIVE', 'WATER')
            
        Returns:
            Dict[str, Any]: Comprehensive trend analysis results
        """
        try:
            # Split device data into two periods for comparison
            period1_data, period2_data = self._split_data_into_weeks(device_data)
            
            # Calculate summary statistics for each period
            period1_stats = self._calculate_period_totals(period1_data)
            period2_stats = self._calculate_period_totals(period2_data)
            
            # Calculate trend variance between the two periods
            trend_metrics = self._calculate_trend_variance(
                period1_stats['total'], 
                period2_stats['total']
            )
            
            # Return comprehensive analysis results
            return {
                'client_name': device_data['client_name'].iloc[0] if 'client_name' in device_data.columns else 'Unknown',
                'device_id': device_id,
                'device_name': device_name,
                'param_key': param_key,
                'analysis_period_start': self.analysis_start_date.isoformat(),
                'analysis_period_end': self.analysis_end_date.isoformat(),
                
                # Period 1 statistics
                'period1_total': round(period1_stats['total'], 2),
                'period1_average': round(period1_stats['average'], 2),
                'period1_count': period1_stats['count'],
                'period1_min': round(period1_stats['min_value'], 2),
                'period1_max': round(period1_stats['max_value'], 2),
                
                # Period 2 statistics
                'period2_total': round(period2_stats['total'], 2),
                'period2_average': round(period2_stats['average'], 2),
                'period2_count': period2_stats['count'],
                'period2_min': round(period2_stats['min_value'], 2),
                'period2_max': round(period2_stats['max_value'], 2),
                
                # Trend analysis results
                'percentage_change': round(trend_metrics['percentage_change'], 2),
                'absolute_difference': round(trend_metrics['absolute_difference'], 2),
                'trend_direction': trend_metrics['trend_direction'],
                'is_flagged': trend_metrics['is_flagged'],
                'threshold_used': self.trend_threshold,
                
                # Analysis metadata
                'total_data_points': len(device_data),
                'analysis_date': datetime.now().isoformat()
            }
            
        except Exception as e:
            # Return error result if analysis fails for this device
            return {
                'client_name': device_data['client_name'].iloc[0] if 'client_name' in device_data.columns else 'Unknown',
                'device_id': device_id,
                'device_name': device_name,
                'param_key': param_key,
                'analysis_period_start': self.analysis_start_date.isoformat(),
                'analysis_period_end': self.analysis_end_date.isoformat(),
                'period1_total': 0.0,
                'period1_average': 0.0,
                'period1_count': 0,
                'period2_total': 0.0,
                'period2_average': 0.0,
                'period2_count': 0,
                'percentage_change': 0.0,
                'absolute_difference': 0.0,
                'trend_direction': 'error',
                'is_flagged': True,
                'threshold_used': self.trend_threshold,
                'total_data_points': len(device_data),
                'analysis_date': datetime.now().isoformat(),
                'error': str(e)
            }
    
    def analyze_trends(self) -> pd.DataFrame:
        """
        Analyze trending patterns for all devices in the configuration.
        
        Returns:
            pd.DataFrame: Complete trend analysis results for all devices
        """
        # Group data by device and parameter for individual analysis
        grouped_data = self.data.groupby(['device_id', 'device_name', 'param_key'])
        
        # Create lookup dictionary for device configuration validation
        device_config = {(d['device_id'], d['param']): d for d in self.config['devices']}
        
        analysis_results = []
        
        print(f"🔍 Starting trend analysis for {len(grouped_data)} device/parameter combinations")
        
        for (device_id, device_name, param_key), group in grouped_data:
            try:
                # Validate that this device/parameter exists in configuration
                device_info = device_config.get((device_id, param_key))
                if not device_info:
                    print(f"Warning: Device {device_id} with parameter {param_key} not found in configuration")
                    continue
                
                # Perform comprehensive trend analysis for this device
                result = self._analyze_device_trend(group, device_id, device_name, param_key)
                analysis_results.append(result)
                
            except Exception as e:
                print(f"Error analyzing device {device_name} (ID: {device_id}): {e}")
                continue
        
        # Store results for report generation and return as DataFrame
        self.results = analysis_results
        
        print(f"✓ Completed trend analysis for {len(analysis_results)} devices")
        
        return pd.DataFrame(analysis_results) if analysis_results else pd.DataFrame()
    
    def save_report(self, output_filename: str) -> None:
        """Save the detailed trend analysis results to a CSV file."""
        if not self.results:
            raise ValueError("No results to save. Run analyze_trends() first.")
        
        # Construct output path in the outputs directory
        output_path = self.output_dir / output_filename
        absolute_path = output_path.resolve()
        
        try:
            df = pd.DataFrame(self.results)
            df.to_csv(absolute_path, index=False, encoding='utf-8')
            
            # Verify file creation and provide feedback on file size
            if absolute_path.exists():
                file_size = absolute_path.stat().st_size
                print(f"✓ Trend analysis report saved to {absolute_path} ({file_size:,} bytes)")
            else:
                print(f"❌ Error: File was not created at {absolute_path}")
                
        except IOError as e:
            raise IOError(f"Error saving CSV report to {absolute_path}: {e}")
    
    def save_text_report(self, output_filename: str) -> None:
        """Generate and save a human-readable text report."""
        if not self.results:
            raise ValueError("No analysis results available. Run analyze_trends() first.")
        
        # Construct output path in the outputs directory
        output_path = self.output_dir / output_filename
        absolute_path = output_path.resolve()
        
        try:
            with open(absolute_path, 'w', encoding='utf-8') as report_file:
                self._write_report_header(report_file)
                self._write_executive_summary(report_file)
                self._write_flagged_devices_section(report_file)
                self._write_detailed_findings(report_file)
                self._write_report_footer(report_file)
            
            # Verify file creation and provide feedback on file size
            if absolute_path.exists():
                file_size = absolute_path.stat().st_size
                print(f"✓ Trend analysis text report saved to {absolute_path} ({file_size:,} bytes)")
            else:
                print(f"❌ Error: File was not created at {absolute_path}")
            
        except IOError as e:
            raise IOError(f"Error saving text report to {absolute_path}: {e}")
    
    def _write_report_header(self, file_handle) -> None:
        """Write the report header with analysis parameters and metadata."""
        file_handle.write("=" * 80 + "\n")
        file_handle.write("DATA TREND ANALYSIS REPORT\n")
        file_handle.write("=" * 80 + "\n\n")
        
        file_handle.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        file_handle.write(f"Analysis Period: {self.analysis_start_date} to {self.analysis_end_date}\n")
        file_handle.write(f"Trend Threshold: {self.trend_threshold}%\n")
        file_handle.write(f"Total Devices Analyzed: {len(self.results)}\n\n")
    
    def _write_executive_summary(self, file_handle) -> None:
        """Write executive summary with key trend metrics."""
        if not self.results:
            return
        
        # Calculate summary statistics for executive overview
        total_devices = len(self.results)
        flagged_devices = len([r for r in self.results if r['is_flagged']])
        stable_devices = len([r for r in self.results if r['trend_direction'] == 'stable'])
        
        file_handle.write("EXECUTIVE SUMMARY\n")
        file_handle.write("-" * 40 + "\n")
        file_handle.write(f"Total Devices Analyzed: {total_devices}\n")
        file_handle.write(f"Devices with Significant Trends: {flagged_devices}\n")
        file_handle.write(f"Devices with Stable Consumption: {stable_devices}\n")
        
        if flagged_devices > 0:
            flagged_results = [r for r in self.results if r['is_flagged']]
            avg_change = sum(abs(r['percentage_change']) for r in flagged_results) / flagged_devices
            file_handle.write(f"Average Change in Flagged Devices: {avg_change:.1f}%\n")
        
        file_handle.write("\n")
    
    def _write_flagged_devices_section(self, file_handle) -> None:
        """Write detailed information about devices with significant trends."""
        flagged_results = [r for r in self.results if r['is_flagged']]
        
        if not flagged_results:
            file_handle.write("FLAGGED DEVICES: None\n")
            file_handle.write("All devices show stable consumption patterns within the configured threshold.\n\n")
            return
        
        file_handle.write(f"FLAGGED DEVICES ({len(flagged_results)} devices exceed {self.trend_threshold}% threshold)\n")
        file_handle.write("-" * 60 + "\n\n")
        
        # Sort by percentage change magnitude for priority reporting
        flagged_results = sorted(flagged_results, key=lambda x: abs(x['percentage_change']), reverse=True)
        
        for result in flagged_results:
            # Calculate period dates for better labeling
            start_date = datetime.fromisoformat(result['analysis_period_start'])
            end_date = datetime.fromisoformat(result['analysis_period_end'])
            
            # Calculate midpoint to determine period boundaries
            total_duration = end_date - start_date
            midpoint_date = start_date + (total_duration / 2)
            
            # Format dates as dd_mm_yy
            period1_start = start_date.strftime('%d_%m_%y')
            period1_end = midpoint_date.strftime('%d_%m_%y')
            period2_start = midpoint_date.strftime('%d_%m_%y')
            period2_end = end_date.strftime('%d_%m_%y')
            
            file_handle.write(f"Device: {result['device_name']}\n")
            file_handle.write(f"  Parameter: {result['param_key']}\n")
            file_handle.write(f"  Period 1 Total ({period1_start} to {period1_end}): {result['period1_total']:.2f}\n")
            file_handle.write(f"  Period 2 Total ({period2_start} to {period2_end}): {result['period2_total']:.2f}\n")
            file_handle.write(f"  Change: {result['percentage_change']:+.1f}% ({result['trend_direction']})\n")
            file_handle.write(f"  Absolute Difference: {result['absolute_difference']:+.2f}\n\n")
        
    def _write_detailed_findings(self, file_handle) -> None:
        """Write detailed analysis findings and trend statistics."""
        file_handle.write("DETAILED ANALYSIS FINDINGS\n")
        file_handle.write("-" * 40 + "\n")
        
        if not self.results:
            file_handle.write("No analysis results available.\n\n")
            return
        
        # Trend direction distribution
        trend_directions = defaultdict(int)
        for result in self.results:
            trend_directions[result['trend_direction']] += 1
        
        file_handle.write("Trend Direction Distribution:\n")
        total_devices = len(self.results)
        for direction, count in trend_directions.items():
            percentage = (count / total_devices) * 100
            file_handle.write(f"  {direction.replace('_', ' ').title()}: {count} devices ({percentage:.1f}%)\n")
        
        # Calculate overall statistics
        percentage_changes = [r['percentage_change'] for r in self.results if r['percentage_change'] != 999.99]
        if percentage_changes:
            avg_change = sum(percentage_changes) / len(percentage_changes)
            max_increase = max(percentage_changes)
            min_decrease = min(percentage_changes)
            
            file_handle.write(f"\nAverage Period-over-Period Change: {avg_change:.1f}%\n")
            file_handle.write(f"Largest Increase: {max_increase:.1f}%\n")
            file_handle.write(f"Largest Decrease: {min_decrease:.1f}%\n\n")
    
    def _write_report_footer(self, file_handle) -> None:
        """Write report footer with methodology and recommendations."""
        file_handle.write("METHODOLOGY\n")
        file_handle.write("-" * 20 + "\n")
        file_handle.write("This analysis compares total consumption between two consecutive time periods.\n")
        file_handle.write(f"Devices showing changes greater than {self.trend_threshold}% are flagged for review.\n")
        file_handle.write("Trends may indicate equipment issues, usage pattern changes, or data quality problems.\n\n")
        
        file_handle.write("RECOMMENDATIONS\n")
        file_handle.write("-" * 20 + "\n")
        file_handle.write("• Investigate devices with significant increases for potential equipment issues\n")
        file_handle.write("• Review devices with significant decreases for operational changes\n")
        file_handle.write("• Monitor flagged devices for continued trend patterns\n")
        file_handle.write("• Consider seasonal factors when interpreting trend changes\n")
        file_handle.write("• Implement automated trend monitoring for early detection\n\n")
        
        file_handle.write("=" * 80 + "\n")
        file_handle.write("End of Report\n")
    
    def print_summary(self) -> None:
        """Print a concise summary of trend analysis results to the console."""
        if not self.results:
            print("No analysis results available. Run analyze_trends() first.")
            return
        
        print("\n" + "=" * 70)
        print("DATA TREND ANALYSIS SUMMARY")
        print("=" * 70)
        
        # Calculate and display overall statistics
        total_devices = len(self.results)
        flagged_devices = len([r for r in self.results if r['is_flagged']])
        stable_devices = len([r for r in self.results if r['trend_direction'] == 'stable'])
        
        print(f"Analysis Period: {self.analysis_start_date} to {self.analysis_end_date}")
        print(f"Total Devices Analyzed: {total_devices}")
        print(f"Devices Flagged for Review: {flagged_devices}")
        print(f"Devices with Stable Trends: {stable_devices}")
        print(f"Trend Threshold Used: {self.trend_threshold}%")
        
        # Show most significant changes if any devices are flagged
        if flagged_devices > 0:
            flagged_results = [r for r in self.results if r['is_flagged']]
            top_changes = sorted(flagged_results, key=lambda x: abs(x['percentage_change']), reverse=True)[:5]
            
            print(f"\nTop 5 Most Significant Changes:")
            for i, result in enumerate(top_changes, 1):
                print(f"  {i}. {result['device_name']}: {result['percentage_change']:+.1f}%")
        
        print("=" * 70)

