#!/usr/bin/env python3
"""
Out-of-Hours Consumption Analysis Tool - COMPLETE FIXED VERSION

This module analyzes energy consumption patterns during working hours versus
out-of-hours periods using standardized Dexcell extractor output. It identifies
problematic consumption patterns and flags devices with excessive after-hours usage.

CRITICAL FIX: Uses config dates to determine analysis periods and proper timezone handling.
"""

import json
import pandas as pd
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from dateutil import parser


class OutOfHoursConsumptionAnalyzer:
    """
    Analyzes out-of-hours energy consumption patterns for Dexcell extracted data.
    
    This class processes hourly consumption data to identify devices with excessive
    energy usage during non-working hours (before 7 AM, after 7 PM, weekends).
    It flags devices that show problematic consumption patterns that may indicate
    equipment issues, security concerns, or operational inefficiencies.
    
    The analyzer handles timezone conversions, generates detailed consumption reports,
    and flags devices based on configurable criteria for operational review.
    
    CRITICAL FIX: Uses configuration dates to determine analysis periods.
    
    Attributes:
        config_path (Path): Path to the JSON configuration file
        csv_path (Path): Path to the CSV data file
        output_dir (Path): Directory for saving output reports
        config (Dict): Loaded configuration data
        data (pd.DataFrame): Processed consumption data
        results (List[Dict[str, Any]]): Analysis results for flagged devices
        working_hours_start (time): Start of working hours (7:00 AM)
        working_hours_end (time): End of working hours (7:00 PM)
        out_of_hours_threshold (float): Percentage threshold for flagging
        analysis_start_date (datetime): Start date for the analysis period
        analysis_end_date (datetime): End date for the analysis period
        
    Example:
        analyzer = OutOfHoursConsumptionAnalyzer(
            config_filename="client_config.json",
            csv_filename="hourly_data.csv",
            out_of_hours_threshold=30.0
        )
        results_df = analyzer.analyze_consumption()
        analyzer.save_report("out_of_hours_analysis.csv")
        analyzer.save_text_report("out_of_hours_summary.txt")
    """
    
    def __init__(
        self, 
        config_filename: str, 
        csv_filename: str, 
        out_of_hours_threshold: float = 30.0
    ) -> None:
        """
        Initialize the out-of-hours consumption analyzer with configuration and data files.
        
        Sets up file paths, loads configuration and data, and calculates the analysis
        period based on the configuration file dates.
        
        Args:
            config_filename: Name of JSON config file in 'client_configs' folder
            csv_filename: Name of CSV data file in 'outputs' folder
            out_of_hours_threshold: Percentage threshold for flagging out-of-hours consumption
        
        Raises:
            ValueError: If threshold is outside valid range (0-100)
            FileNotFoundError: If config or CSV files cannot be found
            ValueError: If required configuration fields are missing or data format is invalid
        
        Example:
            analyzer = OutOfHoursConsumptionAnalyzer("config.json", "data.csv", 25.0)
        """
        # Validate threshold parameter
        if not (0 <= out_of_hours_threshold <= 100):
            raise ValueError("Out-of-hours threshold must be between 0 and 100 percent")
            
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
        
        # Define working hours boundaries and threshold
        self.working_hours_start: time = time(7, 0, 0)  # 7:00 AM
        self.working_hours_end: time = time(19, 0, 0)   # 7:00 PM
        self.out_of_hours_threshold: float = out_of_hours_threshold
        
        # CRITICAL FIX: Use config dates to determine analysis periods
        self.analysis_start_date: datetime = self._parse_config_date(self.config['start_date'])
        self.analysis_end_date: datetime = self._parse_config_date(self.config['end_date'])
        
        # DEBUG: Display analysis period for verification
        print(f"🔍 Out-of-hours analysis period:")
        print(f"    Start: {self.analysis_start_date}")
        print(f"    End: {self.analysis_end_date}")
        print(f"    Working hours: {self.working_hours_start.strftime('%H:%M')} - {self.working_hours_end.strftime('%H:%M')}")
        
        # Prepare data with additional columns needed for time-based analysis
        self._prepare_time_analysis()
    
    def _load_config(self, config_file_path: str) -> Dict:
        """
        Load and validate the JSON configuration file for out-of-hours analysis.
        
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
        Load and prepare the CSV data for out-of-hours analysis.
        
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
        
        # Validate that all required columns exist for time-based analysis
        required_columns = ['device_id', 'device_name', 'param_key', 'timestamp', 'value']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in CSV data: {missing_columns}")
        
        # CRITICAL FIX: Proper timezone handling to prevent time misclassification
        try:
            # Parse timestamps with UTC handling for mixed timezone data
            data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)
            
            # CRITICAL FIX: Add offset to handle timezone conversion properly
            # This ensures that timestamps don't shift to wrong hours
            data['timestamp'] = data['timestamp'] + pd.Timedelta(hours=1)
            
            # Convert to naive datetime after applying offset
            data['timestamp'] = data['timestamp'].dt.tz_convert(None)
            
            # DEBUG: Show sample of processed timestamps
            print(f"🔍 Sample processed timestamps:")
            for i, row in data.head(3).iterrows():
                print(f"    {row['device_name']}: {row['timestamp']} ({row['timestamp'].strftime('%H:%M')})")
            
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format in data: {e}")
        
        return data
    
    def _prepare_time_analysis(self) -> None:
        """
        Prepare data with additional time-based columns for consumption analysis.
        
        Creates time-based identifiers, filters data to the analysis period,
        and adds working hours classification columns.
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
        
        # Extract date and time components for analysis
        self.data['date'] = self.data['timestamp'].dt.date
        self.data['time_only'] = self.data['timestamp'].dt.time
        
        # Create boolean flag for working hours (7 AM to 7 PM)
        self.data['is_working_hours'] = (
            (self.data['time_only'] >= self.working_hours_start) &
            (self.data['time_only'] < self.working_hours_end)
        )
        
        # Sort data for consistent processing and analysis
        self.data = self.data.sort_values(['device_id', 'param_key', 'timestamp'])
    
    def _calculate_daily_consumption(self, device_data: pd.DataFrame) -> Dict[str, float]:
        """
        Calculate working hours and out-of-hours consumption for a device on a specific day.
        
        Args:
            device_data: DataFrame containing one day of data for a single device/parameter
            
        Returns:
            Dictionary containing consumption calculations and analysis flags
        """
        if device_data.empty:
            return {
                'working_hours_consumption': 0.0,
                'out_of_hours_consumption': 0.0,
                'total_consumption': 0.0,
                'out_of_hours_percentage': 0.0,
                'data_points_working': 0,
                'data_points_out_of_hours': 0
            }
        
        # Calculate working hours consumption (7 AM - 7 PM)
        working_hours_data = device_data[device_data['is_working_hours'] == True]
        working_hours_consumption = float(working_hours_data['value'].sum())
        data_points_working = len(working_hours_data)
        
        # Calculate out-of-hours consumption (before 7 AM, after 7 PM)
        out_of_hours_data = device_data[device_data['is_working_hours'] == False]
        out_of_hours_consumption = float(out_of_hours_data['value'].sum())
        data_points_out_of_hours = len(out_of_hours_data)
        
        # Calculate total daily consumption
        total_consumption = working_hours_consumption + out_of_hours_consumption
        
        # Calculate out-of-hours percentage of total consumption
        if total_consumption > 0:
            out_of_hours_percentage = (out_of_hours_consumption / total_consumption) * 100
        else:
            out_of_hours_percentage = 0.0
        
        return {
            'working_hours_consumption': working_hours_consumption,
            'out_of_hours_consumption': out_of_hours_consumption,
            'total_consumption': total_consumption,
            'out_of_hours_percentage': out_of_hours_percentage,
            'data_points_working': data_points_working,
            'data_points_out_of_hours': data_points_out_of_hours
        }
    
    def _identify_consumption_issues(self, consumption_stats: Dict[str, float]) -> Tuple[bool, List[str]]:
        """
        Identify consumption issues based on out-of-hours vs working hours patterns.
        
        Args:
            consumption_stats: Dictionary containing calculated consumption statistics
            
        Returns:
            Tuple of (is_flagged, list_of_issues)
        """
        issues = []
        
        working_hours = consumption_stats['working_hours_consumption']
        out_of_hours = consumption_stats['out_of_hours_consumption']
        out_of_hours_pct = consumption_stats['out_of_hours_percentage']
        
        # Check if out-of-hours consumption exceeds working hours consumption
        if out_of_hours > working_hours:
            issues.append("Out-of-hours consumption exceeds working hours consumption")
        
        # Check if out-of-hours consumption exceeds the percentage threshold
        if out_of_hours_pct > self.out_of_hours_threshold:
            issues.append(f"Out-of-hours consumption exceeds {self.out_of_hours_threshold}% threshold")
        
        # Flag if any issues were identified
        is_flagged = len(issues) > 0
        
        return is_flagged, issues
    
    def _analyze_device_consumption(
        self, 
        device_data: pd.DataFrame, 
        device_id: int,
        device_name: str, 
        param_key: str,
        analysis_date: str
    ) -> Optional[Dict[str, Any]]:
        """
        Analyze out-of-hours consumption for a single device on a specific day.
        
        Args:
            device_data: DataFrame containing data for this specific device/parameter/date
            device_id: Unique identifier for the device
            device_name: Human-readable name of the device
            param_key: Parameter being analyzed (e.g., 'EACTIVE', 'WATER')
            analysis_date: Date being analyzed
            
        Returns:
            Dict[str, Any]: Consumption analysis results if flagged, None otherwise
        """
        try:
            # Calculate daily consumption statistics
            consumption_stats = self._calculate_daily_consumption(device_data)
            
            # Skip days with no consumption data
            if consumption_stats['total_consumption'] == 0:
                return None
            
            # Identify consumption issues based on flagging criteria
            is_flagged, issues = self._identify_consumption_issues(consumption_stats)
            
            # Only return results for flagged devices
            if is_flagged:
                return {
                    'client_name': device_data['client_name'].iloc[0] if 'client_name' in device_data.columns else 'Unknown',
                    'analysis_date': analysis_date,
                    'device_id': device_id,
                    'device_name': device_name,
                    'param_key': param_key,
                    'analysis_period_start': self.analysis_start_date.isoformat(),
                    'analysis_period_end': self.analysis_end_date.isoformat(),
                    'total_consumption': round(consumption_stats['total_consumption'], 2),
                    'working_hours_consumption': round(consumption_stats['working_hours_consumption'], 2),
                    'out_of_hours_consumption': round(consumption_stats['out_of_hours_consumption'], 2),
                    'out_of_hours_percentage': round(consumption_stats['out_of_hours_percentage'], 2),
                    'data_points_working': consumption_stats['data_points_working'],
                    'data_points_out_of_hours': consumption_stats['data_points_out_of_hours'],
                    'issues_identified': issues,
                    'is_flagged': True,
                    'threshold_used': self.out_of_hours_threshold,
                    'analysis_timestamp': datetime.now().isoformat()
                }
            
            return None
            
        except Exception as e:
            print(f"Error analyzing device {device_name} on {analysis_date}: {e}")
            return None
    
    def analyze_consumption(self) -> pd.DataFrame:
        """
        Analyze out-of-hours consumption patterns for all devices across all days.
        
        Returns:
            pd.DataFrame: Complete analysis results for flagged devices
        """
        # Group data by date, device, and parameter for daily analysis
        grouped_data = self.data.groupby(['date', 'device_id', 'device_name', 'param_key'])
        
        # Create lookup dictionary for device configuration validation
        device_config = {(d['device_id'], d['param']): d for d in self.config['devices']}
        
        analysis_results = []
        
        print(f"🔍 Starting out-of-hours analysis for {len(grouped_data)} device/date combinations")
        
        for (analysis_date, device_id, device_name, param_key), group in grouped_data:
            try:
                # Validate that this device/parameter exists in configuration
                device_info = device_config.get((device_id, param_key))
                if not device_info:
                    print(f"Warning: Device {device_id} with parameter {param_key} not found in configuration")
                    continue
                
                # Perform out-of-hours consumption analysis for this device/date
                result = self._analyze_device_consumption(group, device_id, device_name, param_key, str(analysis_date))
                
                if result is not None:
                    analysis_results.append(result)
                    
            except Exception as e:
                print(f"Error analyzing device {device_name} on {analysis_date}: {e}")
                continue
        
        # Store results for report generation and return as DataFrame
        self.results = analysis_results
        
        print(f"✓ Completed out-of-hours analysis. Found {len(analysis_results)} flagged device-days")
        
        return pd.DataFrame(analysis_results) if analysis_results else pd.DataFrame()
    
    def save_report(self, output_filename: str) -> None:
        """Save the detailed out-of-hours consumption analysis results to a CSV file."""
        if not self.results:
            raise ValueError("No results to save. Run analyze_consumption() first.")
        
        # Construct output path in the outputs directory
        output_path = self.output_dir / output_filename
        absolute_path = output_path.resolve()
        
        try:
            df = pd.DataFrame(self.results)
            df.to_csv(absolute_path, index=False, encoding='utf-8')
            
            # Verify file creation and provide feedback on file size
            if absolute_path.exists():
                file_size = absolute_path.stat().st_size
                print(f"✓ Out-of-hours consumption report saved to {absolute_path} ({file_size:,} bytes)")
            else:
                print(f"❌ Error: File was not created at {absolute_path}")
                
        except IOError as e:
            raise IOError(f"Error saving CSV report to {absolute_path}: {e}")
    
    def save_text_report(self, output_filename: str) -> None:
        """Generate and save a human-readable text report."""
        if not self.results:
            # Create a report even if no issues found
            output_path = self.output_dir / output_filename
            absolute_path = output_path.resolve()
            
            try:
                with open(absolute_path, 'w', encoding='utf-8') as report_file:
                    self._write_report_header(report_file)
                    self._write_executive_summary(report_file)
                    self._write_report_footer(report_file)
                
                print(f"✓ Out-of-hours consumption text report saved to {absolute_path}")
                return
                
            except IOError as e:
                raise IOError(f"Error saving text report to {absolute_path}: {e}")
        
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
                print(f"✓ Out-of-hours consumption text report saved to {absolute_path} ({file_size:,} bytes)")
            else:
                print(f"❌ Error: File was not created at {absolute_path}")
            
        except IOError as e:
            raise IOError(f"Error saving text report to {absolute_path}: {e}")
    
    def _write_report_header(self, file_handle) -> None:
        """Write the report header with analysis parameters and metadata."""
        file_handle.write("=" * 80 + "\n")
        file_handle.write("OUT-OF-HOURS CONSUMPTION ANALYSIS REPORT\n")
        file_handle.write("=" * 80 + "\n\n")
        
        file_handle.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        file_handle.write(f"Analysis Period: {self.analysis_start_date} to {self.analysis_end_date}\n")
        file_handle.write(f"Working Hours: {self.working_hours_start.strftime('%H:%M')} - {self.working_hours_end.strftime('%H:%M')}\n")
        file_handle.write(f"Out-of-Hours Threshold: {self.out_of_hours_threshold}%\n")
        file_handle.write(f"Total Flagged Device-Days: {len(self.results)}\n\n")
    
    def _write_executive_summary(self, file_handle) -> None:
        """Write executive summary with key consumption metrics."""
        if not self.results:
            file_handle.write("EXECUTIVE SUMMARY\n")
            file_handle.write("-" * 40 + "\n")
            file_handle.write("No devices found with problematic out-of-hours consumption patterns.\n")
            file_handle.write("All monitored devices show acceptable consumption during working hours.\n\n")
            return
        
        # Calculate summary statistics
        total_flagged = len(self.results)
        unique_devices = len(set((r['device_id'], r['param_key']) for r in self.results))
        
        avg_out_of_hours_pct = sum(r['out_of_hours_percentage'] for r in self.results) / total_flagged
        
        file_handle.write("EXECUTIVE SUMMARY\n")
        file_handle.write("-" * 40 + "\n")
        file_handle.write(f"Total Flagged Device-Days: {total_flagged}\n")
        file_handle.write(f"Unique Devices with Issues: {unique_devices}\n")
        file_handle.write(f"Average Out-of-Hours Consumption: {avg_out_of_hours_pct:.1f}%\n\n")
        
        # Issue type breakdown
        issue_types = defaultdict(int)
        for result in self.results:
            for issue in result['issues_identified']:
                issue_types[issue] += 1
        
        file_handle.write("Issue Type Breakdown:\n")
        for issue_type, count in issue_types.items():
            file_handle.write(f"  • {issue_type}: {count} occurrences\n")
        file_handle.write("\n")
    
    def _write_flagged_devices_section(self, file_handle) -> None:
        """Write detailed information about devices with out-of-hours consumption issues."""
        file_handle.write(f"FLAGGED DEVICES ({len(self.results)} device-days require attention)\n")
        file_handle.write("-" * 60 + "\n\n")
        
        # Sort by out-of-hours percentage (highest first) for priority review
        sorted_results = sorted(self.results, key=lambda x: x['out_of_hours_percentage'], reverse=True)
        
        for result in sorted_results:
            file_handle.write(f"Date: {result['analysis_date']}\n")
            file_handle.write(f"Device: {result['device_name']}\n")
            file_handle.write(f"Parameter: {result['param_key']}\n")
            file_handle.write(f"Total Daily Consumption: {result['total_consumption']:.2f}\n")
            file_handle.write(f"Working Hours (7 AM - 7 PM): {result['working_hours_consumption']:.2f}\n")
            file_handle.write(f"Out-of-Hours: {result['out_of_hours_consumption']:.2f} ({result['out_of_hours_percentage']:.1f}%)\n")
            
            file_handle.write("Issues Identified:\n")
            for issue in result['issues_identified']:
                file_handle.write(f"  • {issue}\n")
            
            file_handle.write("\n")
    
    def _write_detailed_findings(self, file_handle) -> None:
        """Write detailed analysis findings and consumption statistics."""
        file_handle.write("DETAILED ANALYSIS FINDINGS\n")
        file_handle.write("-" * 40 + "\n")
        
        # Consumption pattern analysis
        total_consumption = sum(r['total_consumption'] for r in self.results)
        total_working_hours = sum(r['working_hours_consumption'] for r in self.results)
        total_out_of_hours = sum(r['out_of_hours_consumption'] for r in self.results)
        
        if total_consumption > 0:
            overall_out_of_hours_pct = (total_out_of_hours / total_consumption) * 100
        else:
            overall_out_of_hours_pct = 0.0
        
        file_handle.write("Consumption Pattern Summary:\n")
        file_handle.write(f"  Total Consumption (Flagged Devices): {total_consumption:.2f}\n")
        file_handle.write(f"  Working Hours Consumption: {total_working_hours:.2f}\n")
        file_handle.write(f"  Out-of-Hours Consumption: {total_out_of_hours:.2f} ({overall_out_of_hours_pct:.1f}%)\n\n")
        
        # Severity distribution
        high_concern = len([r for r in self.results if r['out_of_hours_percentage'] > 50])
        medium_concern = len([r for r in self.results if 30 < r['out_of_hours_percentage'] <= 50])
        low_concern = len([r for r in self.results if r['out_of_hours_percentage'] <= 30])
        
        file_handle.write("Severity Distribution:\n")
        file_handle.write(f"  High Concern (>50% out-of-hours): {high_concern} device-days\n")
        file_handle.write(f"  Medium Concern (30-50% out-of-hours): {medium_concern} device-days\n")
        file_handle.write(f"  Low Concern (threshold violations): {low_concern} device-days\n\n")
    
    def _write_report_footer(self, file_handle) -> None:
        """Write report footer with methodology and recommendations."""
        file_handle.write("METHODOLOGY\n")
        file_handle.write("-" * 20 + "\n")
        file_handle.write("This analysis compares energy consumption during working hours (7 AM - 7 PM)\n")
        file_handle.write("against out-of-hours consumption for each device on each day.\n\n")
        
        file_handle.write("Flagging Criteria:\n")
        file_handle.write("• Out-of-hours consumption exceeds working hours consumption, OR\n")
        file_handle.write(f"• Out-of-hours consumption exceeds {self.out_of_hours_threshold}% of total daily consumption\n\n")
        
        file_handle.write("RECOMMENDATIONS\n")
        file_handle.write("-" * 20 + "\n")
        file_handle.write("• Review flagged devices for unnecessary after-hours operation\n")
        file_handle.write("• Investigate high out-of-hours consumption for security or efficiency issues\n")
        file_handle.write("• Consider implementing automated shutdown procedures for non-essential equipment\n")
        file_handle.write("• Establish baseline consumption patterns for operational comparison\n")
        file_handle.write("• Monitor trends to identify equipment degradation or operational changes\n\n")
        
        file_handle.write("=" * 80 + "\n")
        file_handle.write("End of Report\n")
    
    def print_summary(self) -> None:
        """Print a concise summary of out-of-hours consumption analysis results to the console."""
        if not self.results:
            print("\n" + "="*70)
            print("OUT-OF-HOURS CONSUMPTION ANALYSIS SUMMARY")
            print("="*70)
            print("No out-of-hours consumption issues identified.")
            print("All devices show acceptable consumption patterns.")
            print(f"Analysis period: {self.analysis_start_date} to {self.analysis_end_date}")
            print(f"Working hours: {self.working_hours_start.strftime('%H:%M')} - {self.working_hours_end.strftime('%H:%M')}")
            print("="*70)
            return
        
        print("\n" + "="*70)
        print("OUT-OF-HOURS CONSUMPTION ANALYSIS SUMMARY")
        print("="*70)
        
        # Calculate and display overall statistics
        total_flagged = len(self.results)
        unique_devices = len(set((r['device_id'], r['param_key']) for r in self.results))
        avg_out_of_hours_pct = sum(r['out_of_hours_percentage'] for r in self.results) / total_flagged
        
        print(f"Analysis Period: {self.analysis_start_date} to {self.analysis_end_date}")
        print(f"Total Flagged Device-Days: {total_flagged}")
        print(f"Unique Devices with Issues: {unique_devices}")
        print(f"Average Out-of-Hours Consumption: {avg_out_of_hours_pct:.1f}%")
        print(f"Working Hours: {self.working_hours_start.strftime('%H:%M')} - {self.working_hours_end.strftime('%H:%M')}")
        
        # Show top 5 worst offenders
        if total_flagged > 0:
            print(f"\nTop 5 Highest Out-of-Hours Consumption:")
            sorted_results = sorted(self.results, key=lambda x: x['out_of_hours_percentage'], reverse=True)
            
            for i, result in enumerate(sorted_results[:5], 1):
                print(f"  {i}. {result['device_name']}: {result['out_of_hours_percentage']:.1f}% on {result['analysis_date']}")
        
        print("="*70)
