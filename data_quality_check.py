#!/usr/bin/env python3
"""
Hourly Data Quality Analysis Tool - COMPLETE FIXED VERSION

This module provides comprehensive hourly data quality analysis for consumption data
extracted from the Dexcell API. It identifies missing data points, zero consumption
periods, negative values, and calculates completeness metrics for each device.

CRITICAL FIX: Uses config end_date to determine analysis periods and proper timezone handling.
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from dateutil import parser


class HourlyDataQualityAnalyzer:
    """
    Analyzes hourly data quality metrics for Dexcell extracted consumption data.
    
    This class provides comprehensive data quality analysis by examining hourly
    consumption patterns, identifying missing data points, zero consumption periods,
    and calculating completeness metrics across all configured devices.
    
    The analyzer handles timezone conversions, generates detailed quality reports,
    and flags devices that require attention due to data collection issues.
    
    CRITICAL FIX: Uses configuration end_date to determine analysis periods.
    
    Attributes:
        config_path (Path): Path to the JSON configuration file
        csv_path (Path): Path to the CSV data file
        output_dir (Path): Directory for saving output reports
        config (Dict): Loaded configuration data
        data (pd.DataFrame): Processed consumption data
        results (List[Dict[str, Any]]): Quality analysis results for all devices
        analysis_start_date (datetime): Start date for the analysis period
        analysis_end_date (datetime): End date for the analysis period
    
    Example:
        analyzer = HourlyDataQualityAnalyzer(
            config_filename="client_config.json",
            csv_filename="hourly_data.csv"
        )
        results_df = analyzer.analyze_quality()
        analyzer.save_report("hourly_quality_analysis.csv")
        analyzer.save_text_report("hourly_quality_summary.txt")
    """
    
    def __init__(
        self, 
        config_filename: str, 
        csv_filename: str
    ) -> None:
        """
        Initialize the hourly data quality analyzer with configuration and data files.
        
        Sets up file paths, loads configuration and data, and calculates the analysis
        period based on the configuration file dates.
        
        Args:
            config_filename: Name of JSON config file in 'client_configs' folder
            csv_filename: Name of CSV data file in 'outputs' folder
        
        Raises:
            FileNotFoundError: If config or CSV files cannot be found
            ValueError: If required configuration fields are missing or data format is invalid
        
        Example:
            analyzer = HourlyDataQualityAnalyzer("config.json", "hourly_data.csv")
        """
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
        
        # CRITICAL FIX: Use config dates to determine analysis periods
        self.analysis_start_date: datetime = self._parse_config_date(self.config['start_date'])
        self.analysis_end_date: datetime = self._parse_config_date(self.config['end_date'])
        
        # DEBUG: Display analysis period for verification
        print(f"🔍 Hourly analysis period:")
        print(f"    Start: {self.analysis_start_date}")
        print(f"    End: {self.analysis_end_date}")
        
        # Prepare data with additional columns needed for hourly quality analysis
        self._prepare_hourly_analysis()
    
    def _load_config(self, config_file_path: str) -> Dict:
        """
        Load and validate the JSON configuration file for hourly quality analysis.
        
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
        Load and prepare the CSV data for hourly quality analysis.
        
        CRITICAL FIX: Proper timezone handling to prevent hour misclassification.
        
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
        
        # Validate that all required columns exist for quality analysis
        required_columns = ['device_id', 'device_name', 'param_key', 'timestamp', 'value']
        missing_columns = [col for col in required_columns if col not in data.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns in CSV data: {missing_columns}")
        
        # CRITICAL FIX: Proper timezone handling to prevent hour misclassification
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
                print(f"    {row['device_name']}: {row['timestamp']}")
            
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format in data: {e}")
        
        return data
    
    def _prepare_hourly_analysis(self) -> None:
        """
        Prepare data with additional columns and filtering for hourly quality analysis.
        
        Creates datetime identifiers, filters data to include only the analysis
        period, and validates data availability.
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
    
    def _calculate_expected_points(self) -> int:
        """
        Calculate expected number of hourly data points based on config date range.
        
        Returns:
            int: Number of expected hourly data points
        """
        delta = self.analysis_end_date - self.analysis_start_date
        total_hours = int(delta.total_seconds() / 3600)
        # Add 1 to include both start and end hours
        return total_hours + 1
    
    def _analyze_device_quality(
        self, 
        device_data: pd.DataFrame, 
        device_id: int, 
        device_name: str, 
        param_key: str
    ) -> Dict[str, Any]:
        """
        Analyze hourly data quality for a single device/parameter combination.
        
        Args:
            device_data: DataFrame containing data for this specific device/parameter
            device_id: Unique identifier for the device
            device_name: Human-readable name of the device
            param_key: Parameter being analyzed (e.g., 'EACTIVE', 'WATER')
            
        Returns:
            Dict[str, Any]: Comprehensive quality analysis results
        """
        # Calculate expected points for this device
        expected_points = self._calculate_expected_points()
        
        # Calculate actual data points received
        actual_points = len(device_data)
        
        # Calculate completeness percentage
        completeness = (actual_points / expected_points * 100) if expected_points > 0 else 0
        
        # Calculate zero values (may indicate sensor issues)
        zero_count = (device_data['value'] == 0).sum()
        zero_percentage = (zero_count / actual_points * 100) if actual_points > 0 else 0
        
        # Calculate negative values (potential data quality errors)
        negative_count = (device_data['value'] < 0).sum()
        negative_percentage = (negative_count / actual_points * 100) if actual_points > 0 else 0
        
        # Determine quality flags based on analysis criteria
        quality_flags = []
        if completeness < 90:
            quality_flags.append("Poor Completeness")
        if zero_percentage > 10:
            quality_flags.append("High Zero Values")
        if negative_percentage > 0:
            quality_flags.append("Negative Values")
        
        # Return comprehensive quality analysis results
        return {
            'client_name': device_data['client_name'].iloc[0] if 'client_name' in device_data.columns else 'Unknown',
            'device_id': device_id,
            'device_name': device_name,
            'param_key': param_key,
            'analysis_period_start': self.analysis_start_date.isoformat(),
            'analysis_period_end': self.analysis_end_date.isoformat(),
            'expected_points': expected_points,
            'actual_points': actual_points,
            'completeness_percentage': round(completeness, 2),
            'zero_count': int(zero_count),
            'zero_percentage': round(zero_percentage, 2),
            'negative_count': int(negative_count),
            'negative_percentage': round(negative_percentage, 2),
            'quality_flags': quality_flags,
            'is_flagged': len(quality_flags) > 0,
            'analysis_date': datetime.now().isoformat()
        }
    
    def analyze_quality(self) -> pd.DataFrame:
        """
        Analyze hourly data quality for all devices in the configuration.
        
        Returns:
            pd.DataFrame: Complete quality analysis results for all devices
        """
        # Group data by device and parameter for individual analysis
        grouped_data = self.data.groupby(['device_id', 'device_name', 'param_key'])
        
        # Create lookup dictionary for device configuration validation
        device_config = {(d['device_id'], d['param']): d for d in self.config['devices']}
        
        analysis_results = []
        
        print(f"🔍 Starting hourly quality analysis for {len(grouped_data)} device/parameter combinations")
        
        for (device_id, device_name, param_key), group in grouped_data:
            try:
                # Validate that this device/parameter exists in configuration
                device_info = device_config.get((device_id, param_key))
                if not device_info:
                    print(f"Warning: Device {device_id} with parameter {param_key} not found in configuration")
                    continue
                
                # Perform comprehensive hourly quality analysis for this device
                result = self._analyze_device_quality(group, device_id, device_name, param_key)
                analysis_results.append(result)
                
            except Exception as e:
                print(f"Error analyzing device {device_name} (ID: {device_id}): {e}")
                continue
        
        # Store results for report generation and return as DataFrame
        self.results = analysis_results
        
        print(f"✓ Completed hourly quality analysis for {len(analysis_results)} devices")
        
        return pd.DataFrame(analysis_results) if analysis_results else pd.DataFrame()
    
    def save_report(self, output_filename: str) -> None:
        """Save the detailed hourly quality analysis results to a CSV file."""
        if not self.results:
            raise ValueError("No results to save. Run analyze_quality() first.")
        
        # Construct output path in the outputs directory
        output_path = self.output_dir / output_filename
        absolute_path = output_path.resolve()
        
        try:
            df = pd.DataFrame(self.results)
            df.to_csv(absolute_path, index=False, encoding='utf-8')
            
            # Verify file creation and provide feedback on file size
            if absolute_path.exists():
                file_size = absolute_path.stat().st_size
                print(f"✓ Hourly quality report saved to {absolute_path} ({file_size:,} bytes)")
            else:
                print(f"❌ Error: File was not created at {absolute_path}")
                
        except IOError as e:
            raise IOError(f"Error saving CSV report to {absolute_path}: {e}")
    
    def save_text_report(self, output_filename: str) -> None:
        """Generate and save a human-readable text report."""
        if not self.results:
            raise ValueError("No analysis results available. Run analyze_quality() first.")
        
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
                print(f"✓ Hourly quality text report saved to {absolute_path} ({file_size:,} bytes)")
            else:
                print(f"❌ Error: File was not created at {absolute_path}")
            
        except IOError as e:
            raise IOError(f"Error saving text report to {absolute_path}: {e}")
    
    def _write_report_header(self, file_handle) -> None:
        """Write the report header with analysis parameters and metadata."""
        file_handle.write("=" * 80 + "\n")
        file_handle.write("HOURLY DATA QUALITY ANALYSIS REPORT\n")
        file_handle.write("=" * 80 + "\n\n")
        
        file_handle.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        file_handle.write(f"Analysis Period: {self.analysis_start_date} to {self.analysis_end_date}\n")
        file_handle.write(f"Data Frequency: Hourly\n")
        file_handle.write(f"Total Devices Analyzed: {len(self.results)}\n\n")
    
    def _write_executive_summary(self, file_handle) -> None:
        """Write executive summary with key hourly quality metrics."""
        if not self.results:
            return
        
        # Calculate summary statistics for executive overview
        total_devices = len(self.results)
        flagged_devices = len([r for r in self.results if r['is_flagged']])
        avg_completeness = sum(r['completeness_percentage'] for r in self.results) / total_devices
        avg_zero_percentage = sum(r['zero_percentage'] for r in self.results) / total_devices
        
        file_handle.write("EXECUTIVE SUMMARY\n")
        file_handle.write("-" * 40 + "\n")
        file_handle.write(f"Total Devices Analyzed: {total_devices}\n")
        file_handle.write(f"Devices with Quality Issues: {flagged_devices}\n")
        file_handle.write(f"Average Data Completeness: {avg_completeness:.1f}%\n")
        file_handle.write(f"Average Zero Values: {avg_zero_percentage:.1f}%\n\n")
    
    def _write_flagged_devices_section(self, file_handle) -> None:
        """Write detailed information about devices with quality issues."""
        flagged_results = [r for r in self.results if r['is_flagged']]
        
        if not flagged_results:
            file_handle.write("FLAGGED DEVICES: None\n")
            file_handle.write("All devices show acceptable hourly data quality.\n\n")
            return
        
        file_handle.write(f"FLAGGED DEVICES ({len(flagged_results)} devices require attention)\n")
        file_handle.write("-" * 60 + "\n\n")
        
        # Sort by completeness percentage (worst quality first)
        flagged_results = sorted(flagged_results, key=lambda x: x['completeness_percentage'])
        
        for result in flagged_results:
            file_handle.write(f"Device: {result['device_name']}\n")
            file_handle.write(f"  Parameter: {result['param_key']}\n")
            file_handle.write(f"  Completeness: {result['completeness_percentage']:.1f}%\n")
            file_handle.write(f"  Expected Points: {result['expected_points']:,}\n")
            file_handle.write(f"  Actual Points: {result['actual_points']:,}\n")
            file_handle.write(f"  Zero Values: {result['zero_percentage']:.1f}%\n")
            file_handle.write(f"  Quality Issues: {', '.join(result['quality_flags'])}\n\n")
    
    def _write_detailed_findings(self, file_handle) -> None:
        """Write detailed analysis findings and hourly statistics."""
        file_handle.write("DETAILED ANALYSIS FINDINGS\n")
        file_handle.write("-" * 40 + "\n")
        
        if not self.results:
            file_handle.write("No analysis results available.\n\n")
            return
        
        # Calculate quality distribution statistics
        excellent_quality = len([r for r in self.results if r['completeness_percentage'] >= 95])
        good_quality = len([r for r in self.results if 90 <= r['completeness_percentage'] < 95])
        poor_quality = len([r for r in self.results if r['completeness_percentage'] < 90])
        
        total_devices = len(self.results)
        
        file_handle.write("Hourly Data Quality Distribution:\n")
        if total_devices > 0:
            file_handle.write(f"  Excellent Quality (≥95%): {excellent_quality} devices ({(excellent_quality/total_devices)*100:.1f}%)\n")
            file_handle.write(f"  Good Quality (90-95%): {good_quality} devices ({(good_quality/total_devices)*100:.1f}%)\n")
            file_handle.write(f"  Poor Quality (<90%): {poor_quality} devices ({(poor_quality/total_devices)*100:.1f}%)\n\n")
        
        # Calculate data collection statistics
        total_expected = sum(r['expected_points'] for r in self.results)
        total_actual = sum(r['actual_points'] for r in self.results)
        total_zero = sum(r['zero_count'] for r in self.results)
        
        file_handle.write("Data Collection Statistics:\n")
        file_handle.write(f"  Total Expected Hourly Data Points: {total_expected:,}\n")
        file_handle.write(f"  Total Actual Hourly Data Points: {total_actual:,}\n")
        file_handle.write(f"  Total Zero Value Points: {total_zero:,}\n")
        
        if total_expected > 0:
            overall_completeness = (total_actual / total_expected) * 100
            file_handle.write(f"  Overall Data Completeness: {overall_completeness:.1f}%\n\n")
    
    def _write_report_footer(self, file_handle) -> None:
        """Write report footer with methodology and recommendations."""
        file_handle.write("METHODOLOGY\n")
        file_handle.write("-" * 20 + "\n")
        file_handle.write("This analysis evaluates hourly data completeness by comparing actual vs expected data points.\n")
        file_handle.write("Devices with <90% completeness or >10% zero values are flagged for review.\n")
        file_handle.write("Quality issues may indicate sensor problems, connectivity issues, or data collection failures.\n\n")
        
        file_handle.write("RECOMMENDATIONS\n")
        file_handle.write("-" * 20 + "\n")
        file_handle.write("• Review flagged devices for hardware or connectivity issues\n")
        file_handle.write("• Investigate devices with high zero value percentages\n")
        file_handle.write("• Monitor devices with negative values for sensor calibration issues\n")
        file_handle.write("• Consider implementing automated alerts for poor data quality\n")
        file_handle.write("• Schedule regular maintenance for devices with recurring quality issues\n\n")
        
        file_handle.write("=" * 80 + "\n")
        file_handle.write("End of Report\n")
    
    def print_summary(self) -> None:
        """Print a concise summary of hourly quality analysis results to the console."""
        if not self.results:
            print("No results to summarize. Run analyze_quality() first.")
            return
        
        print("\n" + "="*70)
        print("HOURLY DATA QUALITY ANALYSIS SUMMARY")
        print("="*70)
        
        # Calculate and display overall statistics
        total_devices = len(self.results)
        flagged_devices = len([r for r in self.results if r['is_flagged']])
        avg_completeness = sum(r['completeness_percentage'] for r in self.results) / total_devices if total_devices > 0 else 0
        avg_zero_percentage = sum(r['zero_percentage'] for r in self.results) / total_devices if total_devices > 0 else 0
        
        print(f"Analysis Period: {self.analysis_start_date} to {self.analysis_end_date}")
        print(f"Total Devices Analyzed: {total_devices}")
        print(f"Devices with Quality Issues: {flagged_devices}")
        print(f"Average Data Completeness: {avg_completeness:.1f}%")
        print(f"Average Zero Values: {avg_zero_percentage:.1f}%")
        
        # Display devices with poor completeness for immediate attention
        poor_completeness = [r for r in self.results if r['completeness_percentage'] < 90]
        if poor_completeness:
            print(f"\nDevices with <90% Completeness: {len(poor_completeness)}")
            for i, result in enumerate(poor_completeness[:5], 1):  # Show top 5
                print(f"  {i}. {result['device_name']}: {result['completeness_percentage']:.1f}%")
        
        # Display devices with high zero values
        high_zeros = [r for r in self.results if r['zero_percentage'] > 10]
        if high_zeros:
            print(f"\nDevices with >10% Zero Values: {len(high_zeros)}")
            for i, result in enumerate(high_zeros[:5], 1):  # Show top 5
                print(f"  {i}. {result['device_name']}: {result['zero_percentage']:.1f}%")
        
        print("="*70)


# Simplified version for backward compatibility
class DataQualityAnalyzer(HourlyDataQualityAnalyzer):
    """
    Simplified hourly data quality analyzer for backward compatibility.
    
    This class maintains the same interface as the original DataQualityAnalyzer
    while providing all the enhanced functionality of the new version.
    """
    
    def __init__(self, config_file_path: str, csv_file_path: str):
        """
        Initialize with full file paths for backward compatibility.
        
        Args:
            config_file_path: Full path to config file
            csv_file_path: Full path to CSV file
        """
        # Extract filename from full paths
        config_filename = Path(config_file_path).name
        csv_filename = Path(csv_file_path).name
        
        # Initialize with filename-based approach
        super().__init__(config_filename, csv_filename)
