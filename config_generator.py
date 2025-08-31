#!/usr/bin/env python3
"""
Simplified Dexcell Configuration Generator

Step 1: Fetch devices from Dexcell API for all clients and parameters
Step 2: Filter out historical/archive devices and group devices  
Step 3: Generate client configuration files in JSON format
Step 4: Generate comprehensive device report in text format
"""

import json
import requests
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set

# --- Configuration ---
API_BASE_URL = "https://api.dexcell.com/v3/devices"
MAX_LIMIT = 1000
TIMEOUT = 30

# Client API tokens
API_TOKENS = {
    'da249a3cea387be4fdf9': 'ILIM'
}

# Parameter keys to query
# Available parameter keys for device queries
PARAM_KEYS: List[str] = [
    "POWER", "EACTIVE", "IRPOWER", "IRENERGY", "VOLTAGE", "CURRENT", "CRPOWER",
    "CRENERGY", "APPOWER", "APENERGY", "COSPHY", "PF", "NCURRENT", "FREQ",
    "IAENERGY", "IIRENERGY", "IAPENERGY", "MAXDEMAND", "THDV", "THDC",
    "IMAXVOLTAGE", "IMAXCURRENT", "AVGCURRENT", "EACTIVEABS", "IRENERGYABS",
    "VOLTAGELL", "TANPHI", "POWER_FCST", "EACTIVE_FCST", "PAPOWER", "PAENERGY",
    "PIRPOWER", "PIRENERGY", "PVOLTAGE", "PCURRENT", "PCRPOWER", "PCRENERGY",
    "PADCPOWER", "PFREQ", "PDCVOLTAGE", "PDCCURRENT", "EACTIVEABSEXP",
    "PAPOWER_FCST", "PAENERGY_FCST", "GASVOLUME", "GASENERGY", "GASVOLN",
    "GASVOLUME_FCST", "GASENERGY_FCST", "GASVOLN_FCST", "HVV", "PTZ", "GASCF",
    "DIFFPRESSURE", "GASFLOWSTDV", "GASFLOWN", "GASVOLS", "LNGMASS", "LNGENERGY",
    "LNGMASS_FCST", "LNGENERGY_FCST", "LPG_MASS", "FUELVOLUME", "FUELENERGY",
    "FUELVOLUME_FCST", "FUELENERGY_FCST", "HEAVY_FUEL_OIL", "GASOLINE_VOLUME",
    "AIRVOLUME", "AIRMASSFLOW", "AIRPRESSURE", "AIRVOLUMEN", "AIRMASSFLOWN",
    "PELLETS", "WOODCHIP", "BIOMASS", "BIOMASSENERGY", "BIOMASS_FCST",
    "BIOMASSENERGY_FCST", "HVM", "STEAMENERGY", "STEAMMASS", "STEAMENERGY_FCST",
    "THERMPOWER", "THERMENERGY", "TENERGYC", "TPOWERC", "THERMENERGY_FCST",
    "GENERICVOLUME", "GENERICENERGY", "GENERICPOWER", "BURNOIL_MASS", "ENERGYN",
    "TOE", "TEMP", "HUMID", "LIGHT", "SOUND", "SOILHUM", "DEWPOINT", "TEMP_FCST",
    "HUMID_FCST", "CDD", "HDD", "DD", "CDD_FCST", "HDD_FCST", "DD_FCST",
    "HEATINDEX", "ABSHUMID", "WINDSPEED", "WINDDIR", "PRECIPITATION", "PRESS",
    "SEALEVPRESS", "CLOUD", "VISIBILITY", "PROBRAIN", "SNOW", "SNOWDEPTH",
    "SOLRAD", "DIRTINESS", "SOLRADDHI", "SOLRADDNI", "SOLRADGHI", "UVINDEX",
    "SOLELEV", "SOLAZIM", "SOLHOUR", "SOLRADENERGY", "SOLRAD_FCST", "AIRCO",
    "AIRCO2", "PM25", "PM10", "TVOC", "PM1", "PM4", "TVOC_MOLAR", "WATERVOL",
    "WATERFLOW", "WATERVOL_FCST", "CARBONEQ", "CARBONDIOX", "METHANE", "NITROOX",
    "OZONE", "CHLOROFLUOR", "SULFURHEX", "NITROGENTRI", "CONDUCTIVITY",
    "MASSFLOWKG", "FLUIDTEMP", "FLUIDTEMPF", "FLUIDPRESSURE", "FLUIDPRESSUREPSI",
    "TOTALCHLORINE", "TOTALCHLORINEMGL", "FREECHLORINE", "FREECHLORINEMGL", "PH",
    "FLUIDBIOFILM", "FLUIDVOL_SUPPLY", "FLUIDVOL_RETURN", "ORP", "TURBIDITY",
    "TANKLEVEL", "TANKPRESS", "TANKTEMP", "TOU", "EFFICIENCY", "IO", "PULSE",
    "DEVICEID", "GROUPID", "RSSI", "HOP", "FIRSTHOP", "LASTHOP", "SAMPLING",
    "NETCHANNEL", "TOTALDEVICES", "BAT", "LQI", "ETX", "AGEVALUES", "BAT_PERCENT",
    "DTEMP", "INVTEMP", "ERRORS", "STATUS", "LOADFACTOR", "SETPOINTTEMP",
    "RELAYTEMP", "SETPOINTTEMP_FCST", "HOTWATERVOL", "MASSFLOW", "INTEMP",
    "OUTTEMP", "COPEER", "LPRESS", "HPRESS", "ASUPPLYTEMP", "ASUPPLYTEMPSET",
    "ASUPPLYPRES", "ASUPPLYPRESSET", "ASUPPLYRELHUM", "ASUPPLYRELHUMSET",
    "ARETURNTEMP", "ARETURNPRES", "ARETURNPRESSET", "ARETURNRELHUM",
    "V3VHEATINGCOIL", "V3VCOOLINGCOIL", "ADAMPERPOS", "ACCUMTEMP", "STARTCOUNTER",
    "HVACSTATUS", "HVACSPEED", "HVACMODE", "GENERIC", "ABSOCP", "RELOCP",
    "ABSOCP_FCST", "RELOCP_FCST", "PROD", "PROD_FCST", "WASTEWEIGHT", "VALVEOPEN",
    "MACHINELOAD", "COMPOSITION", "COMPOSITIONPPM", "DENSITY", "HEIGHT", "TURNE",
    "TURND", "TURNP", "PROFITE", "PROFITD", "ETARIFF"
]

@dataclass
class Device:
    """Simple device data structure"""
    client_name: str
    device_id: str
    name: str
    param_key: str
    status: str
    local_id: str
    is_group: bool = False
    is_historical: bool = False

# Storage for all devices
all_devices: List[Device] = []
active_devices: List[Device] = []
filtered_devices: List[Device] = []

print("🚀 Starting Dexcell Configuration Generation")
print("="*60)

# --- Step 1: Fetch Devices from API ---
print("\n📥 STEP 1: FETCHING DEVICES FROM API")
print("-" * 30)

def fetch_devices_for_param(token: str, client_name: str, param_key: str) -> List[Device]:
    """Fetch devices for a specific parameter key"""
    url = f"{API_BASE_URL}?limit={MAX_LIMIT}&param_key={param_key}"
    headers = {'x-dexcell-token': token}
    
    try:
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        if response.status_code == 200:
            devices = []
            for raw_device in response.json():
                # Create device object
                device = Device(
                    client_name=client_name,
                    device_id=str(raw_device.get('id', '')),
                    name=raw_device.get('name', ''),
                    param_key=param_key,
                    status=raw_device.get('status', ''),
                    local_id=raw_device.get('local_id', '')
                )
                
                # Check if it's a group device
                device.is_group = device.local_id.startswith('G_')
                
                # Check if it's historical/archive device
                name_lower = device.name.lower()
                device.is_historical = ('historical' in name_lower or 'archive' in name_lower)
                
                devices.append(device)
            
            return devices
        else:
            print(f"❌ API error for {param_key}: Status {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Network error for {param_key}: {e}")
        return []

# Fetch all devices
total_requests = len(API_TOKENS) * len(PARAM_KEYS)
current_request = 0

for token, client_name in API_TOKENS.items():
    print(f"📡 Processing client: {client_name}")
    
    for param_key in PARAM_KEYS:
        current_request += 1
        print(f"  Fetching {param_key}... ({current_request}/{total_requests})")
        
        devices = fetch_devices_for_param(token, client_name, param_key)
        all_devices.extend(devices)

print(f"✅ Fetched {len(all_devices)} total devices")

# --- Step 2: Filter Devices ---
print("\n🔍 STEP 2: FILTERING DEVICES")
print("-" * 30)

for device in all_devices:
    if not device.is_historical and not device.is_group:
        active_devices.append(device)
    else:
        filtered_devices.append(device)

print(f"✅ Active devices: {len(active_devices)}")
print(f"🗂️  Filtered out: {len(filtered_devices)} (historical/archive/group devices)")

# --- Step 3: Generate Configuration Files ---
print("\n📄 STEP 3: GENERATING CONFIGURATION FILES")
print("-" * 30)

# Create output directory
output_dir = Path("client_configs")
output_dir.mkdir(exist_ok=True)

# Group devices by client
clients = {}
for device in active_devices:
    if device.client_name not in clients:
        clients[device.client_name] = {
            'devices': [],
            'param_keys': set(),
            'token': None
        }
    
    clients[device.client_name]['devices'].append(device)
    clients[device.client_name]['param_keys'].add(device.param_key)

# Set tokens for each client
token_lookup = {v: k for k, v in API_TOKENS.items()}
for client_name in clients:
    clients[client_name]['token'] = token_lookup.get(client_name, '')

# Generate JSON config for each client
for client_name, client_data in clients.items():
    config = {
        "api_keys": [
            {
                "token": client_data['token'],
                "client_name": client_name
            }
        ],
        "params": sorted(list(client_data['param_keys'])),
        "request_type": "",
        "start_date": "",
        "end_date": "",
        "devices": []
    }
    
    # Add devices
    for device in client_data['devices']:
        config["devices"].append({
            "device_id": int(device.device_id) if device.device_id.isdigit() else device.device_id,
            "name": device.name,
            "param": device.param_key
        })
    
    # Save JSON file
    filename = f"{client_name.lower().replace(' ', '_')}_config.json"
    filepath = output_dir / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    
    print(f"✅ {client_name}: {len(client_data['devices'])} devices → {filename}")

# --- Step 4: Generate Device Report ---
print("\n📊 STEP 4: GENERATING DEVICE REPORT")
print("-" * 30)

report_filename = f"device_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

with open(report_filename, 'w', encoding='utf-8') as f:
    f.write("=" * 80 + "\n")
    f.write("DEXCELL DEVICE ANALYSIS REPORT\n")
    f.write("=" * 80 + "\n\n")
    
    f.write(f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"Total Devices Found: {len(all_devices)}\n")
    f.write(f"Active Devices: {len(active_devices)}\n")
    f.write(f"Filtered Devices: {len(filtered_devices)}\n\n")
    
    # Client breakdown
    f.write("CLIENT BREAKDOWN:\n")
    f.write("-" * 30 + "\n")
    for client_name, client_data in clients.items():
        f.write(f"{client_name}: {len(client_data['devices'])} devices\n")
    
    # Parameter breakdown
    f.write("\nPARAMETER BREAKDOWN:\n")
    f.write("-" * 30 + "\n")
    param_counts = {}
    for device in active_devices:
        param_counts[device.param_key] = param_counts.get(device.param_key, 0) + 1
    
    for param, count in sorted(param_counts.items()):
        f.write(f"{param}: {count} devices\n")
    
    # Status breakdown
    f.write("\nSTATUS BREAKDOWN:\n")
    f.write("-" * 30 + "\n")
    status_counts = {}
    for device in active_devices:
        status = device.status or 'Unknown'
        status_counts[status] = status_counts.get(status, 0) + 1
    
    for status, count in sorted(status_counts.items()):
        f.write(f"{status}: {count} devices\n")
    
    # Filtered devices breakdown
    if filtered_devices:
        f.write("\nFILTERED DEVICES (Historical/Archive/Groups):\n")
        f.write("-" * 50 + "\n")
        
        historical_count = len([d for d in filtered_devices if d.is_historical])
        group_count = len([d for d in filtered_devices if d.is_group])
        
        f.write(f"Historical/Archive devices: {historical_count}\n")
        f.write(f"Group devices: {group_count}\n\n")
        
        f.write("Historical/Archive devices:\n")
        for device in filtered_devices:
            if device.is_historical:
                f.write(f"  {device.device_id}: {device.name} ({device.param_key})\n")
        
        f.write("\nGroup devices:\n")
        for device in filtered_devices:
            if device.is_group:
                f.write(f"  {device.device_id}: {device.name} ({device.param_key})\n")
    
    # All active devices list
    f.write("\nACTIVE DEVICES LIST:\n")
    f.write("-" * 30 + "\n")
    for device in sorted(active_devices, key=lambda x: (x.client_name, x.param_key, x.name)):
        f.write(f"{device.device_id:10} | {device.param_key:12} | {device.name}\n")

print(f"✅ Device report saved: {report_filename}")

# --- Final Summary ---
print("\n" + "="*60)
print("🎉 DEXCELL CONFIGURATION GENERATION COMPLETE")
print("="*60)
print(f"📊 Total devices found: {len(all_devices)}")
print(f"✅ Active devices: {len(active_devices)}")
print(f"🗂️  Filtered devices: {len(filtered_devices)}")
print(f"📁 Configuration files: {len(clients)} clients")
print(f"📋 Device report: {report_filename}")
print("="*60)
