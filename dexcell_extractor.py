#!/usr/bin/env python3
"""
Standardized Dexcell API Data Extractor for Team Use - Enhanced Version

This module provides a unified interface for extracting data from the Dexcell API
using a standardized JSON configuration. Enhanced for better performance with
large configurations while maintaining complete backward compatibility.

Config files are loaded from 'client_configs' folder.
Output files are saved to 'outputs' folder.
"""

import json
import csv
import requests
import time
import logging
import threading
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict


class DexcellDataExtractor:
    """
    Standardized class for extracting data from Dexcell API.
    
    Enhanced version with concurrency and better error handling while
    maintaining complete backward compatibility.
    
    Config files are automatically loaded from 'client_configs' folder.
    Output files are automatically saved to 'outputs' folder.
    """
    
    def __init__(self, config_filename: str, debug: bool = False, max_workers: int = 5):
        """
        Initialize the extractor.
        
        Args:
            config_filename: Name of JSON config file (in ./configs/ folder)
            debug: Enable debug logging
            max_workers: Maximum concurrent API requests
        """
        # CRITICAL: Assign all attributes that might be accessed by other methods FIRST
        self.debug = debug
        self.max_workers = max_workers
        self.results = []
        self._results_lock = threading.Lock()
        
        # Setup logging early
        log_level = logging.DEBUG if self.debug else logging.INFO
        logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        # Get directories
        script_dir = Path(__file__).parent.resolve()
        self.config_dir = script_dir / 'client_configs'
        self.output_dir = script_dir / 'outputs'
        
        # Ensure directories exist
        self.config_dir.mkdir(exist_ok=True)
        self.output_dir.mkdir(exist_ok=True)
        
        # Now safe to load config
        config_path = self.config_dir / config_filename
        self.config = self._load_config(str(config_path))
        
        print(f"✓ Config loaded from: {config_path}")
        print(f"✓ Output directory: {self.output_dir}")

    
    def _load_config(self, config_file_path: str) -> Dict:
        """
        Load and validate the configuration file.
        
        Args:
            config_file_path: Full path to the JSON configuration file
            
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid
        """
        try:
            with open(config_file_path, 'r') as f:
                config = json.load(f)
            
            # Validate required fields
            required_fields = ['api_keys', 'params', 'request_type', 'devices']
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Missing required field in config: {field}")
            
            # Validate request type
            if config['request_type'] not in ['monthly', 'hourly']:
                raise ValueError("request_type must be 'monthly' or 'hourly'")
            
            # Enhanced validation for devices structure
            device_ids = set()
            for device in config['devices']:
                required_device_fields = ['device_id', 'name', 'param']
                for field in required_device_fields:
                    if field not in device:
                        raise ValueError(f"Missing required field in device: {field}")
                
                # Check for duplicate device IDs
                if device['device_id'] in device_ids:
                    if self.debug:
                        self.logger.warning(f"Duplicate device ID found: {device['device_id']}")
                device_ids.add(device['device_id'])
                
                # Validate param is in allowed params
                if device['param'] not in config['params']:
                    if self.debug:
                        self.logger.debug(f"Device {device['name']} uses param {device['param']} "
                                        f"which is not in allowed params list")
            
            return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file not found: {config_file_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in configuration file: {e}")
    
    def _get_headers(self, token: str) -> Dict[str, str]:
        """
        Generate headers for API requests using x-dexcell-token.
        
        Args:
            token: API authentication token
            
        Returns:
            Dictionary containing request headers
        """
        return {
            'x-dexcell-token': token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def _make_request_with_retry(self, url: str, headers: Dict[str, str],
                                params: Dict, timeout: int = 30) -> Optional[Dict]:
        """
        Make HTTP request with enhanced retry logic.
        
        Args:
            url: Request URL
            headers: Request headers
            params: Query parameters
            timeout: Request timeout in seconds
            
        Returns:
            Response JSON or None if all attempts failed
        """
        max_retries = 3
        base_wait = 1
        max_wait = 30
        
        for attempt in range(max_retries):
            try:
                if self.debug:
                    self.logger.debug(f"Attempt {attempt + 1}/{max_retries} for device {params.get('device_id')}")
                
                response = requests.get(url, headers=headers, params=params, timeout=timeout)
                
                if response.status_code == 200:
                    data = response.json()
                    # Validate response structure
                    if 'values' not in data:
                        self.logger.error(f"Invalid response structure for device {params.get('device_id')}")
                        return None
                    return data
                    
                elif response.status_code == 401:
                    self.logger.error(f"401 Unauthorized for device {params.get('device_id')}: {response.text}")
                    self.logger.error("Check your API token and permissions")
                    return None
                    
                elif response.status_code == 429:
                    # Enhanced rate limit handling
                    wait_time = min(base_wait * (3 ** attempt), max_wait)
                    self.logger.warning(f"Rate limited for device {params.get('device_id')}. "
                                      f"Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                    
                elif response.status_code in [500, 502, 503, 504]:
                    # Server errors with exponential backoff
                    wait_time = min(base_wait * (2 ** attempt), max_wait)
                    self.logger.warning(f"Server error {response.status_code} for device "
                                      f"{params.get('device_id')}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                    
                else:
                    self.logger.error(f"HTTP {response.status_code} for device "
                                    f"{params.get('device_id')}: {response.text}")
                    return None
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"Request timeout for device {params.get('device_id')} "
                                  f"(attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(base_wait * (2 ** attempt))
                continue
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request failed for device {params.get('device_id')}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(base_wait * (2 ** attempt))
                continue
        
        self.logger.error(f"All retry attempts failed for device {params.get('device_id')}")
        return None
    
    def _extract_device_data(self, task: Dict) -> List[Dict]:
        """
        Extract data for a single device (worker function for concurrent processing).
        
        Args:
            task: Dictionary containing device extraction parameters
            
        Returns:
            List of extracted data records
        """
        url = "https://api.dexcell.com/v3/readings"
        headers = self._get_headers(task['token'])
        
        # Set resolution based on request type
        resolution = 'H' if self.config['request_type'] == 'hourly' else 'M'
        
        params = {
            'device_id': task['device_id'],
            'operation': 'DELTA',
            'parameter_key': task['param_key'],
            'resolution': resolution,
            'from': self.config['start_date'],
            'to': self.config['end_date']
        }
        
        # Make API request
        response_data = self._make_request_with_retry(url, headers, params)
        
        if response_data and 'values' in response_data:
            # Process the data
            results = []
            for item in response_data['values']:
                # Validate data point structure
                if 'ts' not in item or 'v' not in item:
                    self.logger.warning(f"Skipping invalid data point for device {task['device_name']}")
                    continue
                
                result = {
                    'client_name': task['client_name'],
                    'device_id': task['device_id'],
                    'device_name': task['device_name'],
                    'param_key': task['param_key'],
                    'timestamp': item['ts'],
                    'value': item['v'],
                    'extraction_date': datetime.now().isoformat()
                }
                results.append(result)
            
            if results:
                self.logger.info(f"Successfully extracted {len(results)} records for device {task['device_name']}")
            return results
        else:
            self.logger.error(f"Failed to fetch data for device {task['device_name']}")
            return []
    
    def extract_data(self) -> List[Dict]:
        """
        Extract data for all configured devices using concurrent processing.
        
        Returns:
            List of dictionaries containing extracted data
        """
        # Create extraction tasks
        tasks = []
        for api_key_info in self.config['api_keys']:
            token = api_key_info['token']
            client_name = api_key_info.get('client_name', 'Unknown Client')
            
            for device in self.config['devices']:
                device_id = device['device_id']
                device_name = device['name']
                param_key = device['param']
                
                # Check if param is allowed
                if param_key not in self.config['params']:
                    if self.debug:
                        self.logger.debug(f"Skipping device {device_name} - param {param_key} not allowed")
                    continue
                
                task = {
                    'token': token,
                    'client_name': client_name,
                    'device_id': device_id,
                    'device_name': device_name,
                    'param_key': param_key
                }
                tasks.append(task)
        
        total_tasks = len(tasks)
        self.logger.info(f"Starting extraction of {total_tasks} tasks using {self.max_workers} workers")
        
        # Process tasks concurrently
        all_results = []
        failed_tasks = []
        
        if self.max_workers == 1:
            # Sequential processing (backward compatibility for debug/single-threaded)
            for i, task in enumerate(tasks):
                if self.debug and i % 10 == 0:
                    print(f"Processing task {i+1}/{total_tasks}: {task['device_name']}")
                
                results = self._extract_device_data(task)
                if results:
                    all_results.extend(results)
                else:
                    failed_tasks.append(task)
        else:
            # Concurrent processing for better performance
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_task = {
                    executor.submit(self._extract_device_data, task): task 
                    for task in tasks
                }
                
                # Collect results as they complete
                completed = 0
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    completed += 1
                    
                    if self.debug and completed % 10 == 0:
                        print(f"Completed {completed}/{total_tasks} tasks")
                    
                    try:
                        results = future.result()
                        if results:
                            with self._results_lock:
                                all_results.extend(results)
                        else:
                            failed_tasks.append(task)
                    except Exception as e:
                        self.logger.error(f"Unexpected error processing {task['device_name']}: {e}")
                        failed_tasks.append(task)
        
        # Store results
        self.results = all_results
        
        # Summary logging
        success_rate = ((total_tasks - len(failed_tasks)) / total_tasks * 100) if total_tasks > 0 else 0
        self.logger.info(f"Extraction completed:")
        self.logger.info(f"  Total tasks: {total_tasks}")
        self.logger.info(f"  Successful: {total_tasks - len(failed_tasks)}")
        self.logger.info(f"  Failed: {len(failed_tasks)}")
        self.logger.info(f"  Success rate: {success_rate:.1f}%")
        self.logger.info(f"  Total records extracted: {len(all_results)}")
        
        if failed_tasks and self.debug:
            self.logger.warning("Failed devices (first 5):")
            for task in failed_tasks[:5]:
                self.logger.warning(f"  - {task['device_name']} ({task['client_name']})")
        
        return all_results
    
    def save_to_csv(self, output_filename: str):
        """
        Save extracted data to CSV file in the 'outputs' folder.
        
        Args:
            output_filename: Filename for the output CSV file (saved to 'outputs' folder)
        """
        if not self.results:
            message = "No data to save. Run extract_data() first."
            print(message)
            self.logger.warning(message)
            return
        
        # Setup output directory and path
        base_dir = Path(__file__).parent
        output_dir = base_dir / 'outputs'
        
        # Ensure output directory exists
        output_dir.mkdir(exist_ok=True)
        
        # Construct full output path
        output_path = output_dir / output_filename
        
        # Convert to absolute path for clarity
        absolute_path = output_path.resolve()
        
        fieldnames = [
            'client_name', 'device_id', 'device_name', 'param_key',
            'timestamp', 'value', 'extraction_date'
        ]
        
        try:
            with open(absolute_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.results)
            
            # Verify file was created and get file size
            if absolute_path.exists():
                file_size = absolute_path.stat().st_size
                success_message = f"Data successfully saved to {absolute_path} ({file_size:,} bytes)"
                print(success_message)
                self.logger.info(success_message)
            else:
                error_message = f"File was not created at {absolute_path}"
                print(error_message)
                self.logger.error(error_message)
            
        except IOError as e:
            error_message = f"Error saving to CSV: {e}"
            print(error_message)
            self.logger.error(error_message)
            raise
    
    def get_summary(self) -> Dict:
        """
        Get summary of extracted data.
        
        Returns:
            Dictionary containing summary information
        """
        if not self.results:
            return {"message": "No data extracted yet. Run extract_data() first."}
        
        clients = set(result['client_name'] for result in self.results)
        devices = set(result['device_id'] for result in self.results)
        params = set(result['param_key'] for result in self.results)
        
        # Calculate success metrics
        total_expected_devices = len(self.config['devices']) if self.config.get('devices') else 0
        actual_devices_with_data = len(devices)
        
        summary = {
            "total_records": len(self.results),
            "clients": list(clients),
            "devices_count": len(devices),
            "params": list(params),
            "extraction_date": datetime.now().isoformat()
        }
        
        # Add success rate if we have device info
        if total_expected_devices > 0:
            success_rate = (actual_devices_with_data / total_expected_devices) * 100
            summary["device_success_rate"] = f"{success_rate:.1f}%"
        
        return summary
