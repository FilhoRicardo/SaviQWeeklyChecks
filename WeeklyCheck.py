#!/usr/bin/env python3
"""
Hourly Data Analysis Workflow Orchestrator - CORRECTED VERSION

This orchestrator uses the FIXED analyzer classes with consistent interfaces:
- Uses filename-based parameters (not full paths)
- Uses client_configs/ and outputs/ directory structure
- Uses corrected timezone handling and config date parsing
"""

# --- Import FIXED classes ---
from dexcell_extractor import DexcellDataExtractor
from data_quality_check import HourlyDataQualityAnalyzer
from trend_check import DataTrendAnalyzer
from working_hours_check import OutOfHoursConsumptionAnalyzer 

# --- Configuration and file names ---
CONFIG_FILE = "kells_14days_hourly_config.json"  # In client_configs/ folder
OUTPUT_CSV = "extracted_hourly_data.csv"         # Saved to outputs/ folder automatically

# Quality analysis reports
QUALITY_CSV = "hourly_quality_report.csv"
QUALITY_TXT = "hourly_quality_summary.txt"

# Trend analysis reports  
TREND_CSV = "trend_analysis_report.csv"
TREND_TXT = "trend_analysis_summary.txt"

# Out-of-hours analysis reports
OUT_OF_HOURS_CSV = "out_of_hours_analysis.csv"
OUT_OF_HOURS_TXT = "out_of_hours_summary.txt"

print("🚀 Starting Hourly Data Analysis Workflow")
print("="*60)

# --- STEP 1: Data Extraction ---
print("\n📥 STEP 1: DATA EXTRACTION")
print("-" * 30)

try:
    # Use FIXED DexcellDataExtractor (filename-based)
    extractor = DexcellDataExtractor(CONFIG_FILE, debug=True)
    
    # Extract data from API
    data = extractor.extract_data()
    
    # Save to CSV (automatically goes to outputs/ folder)
    extractor.save_to_csv(OUTPUT_CSV)
    print(f"✅ Data extracted and saved to {OUTPUT_CSV}")
    
    # Show extraction summary
    summary = extractor.get_summary()
    print(f"📊 Extraction Summary: {summary}")
    
except Exception as e:
    print(f"❌ Data extraction failed: {e}")
    exit(1)

# --- STEP 2: Hourly Data Quality Analysis ---
print("\n🔍 STEP 2: HOURLY DATA QUALITY ANALYSIS")
print("-" * 30)

try:
    # Use FIXED HourlyDataQualityAnalyzer (filename-based, uses config dates)
    quality_analyzer = HourlyDataQualityAnalyzer(
        config_filename=CONFIG_FILE,  # Just filename, not full path
        csv_filename=OUTPUT_CSV       # Just filename, not full path
    )
    
    # Run quality analysis
    quality_report = quality_analyzer.analyze_quality()
    
    # Save reports using FIXED methods
    quality_analyzer.save_report(QUALITY_CSV)
    quality_analyzer.save_text_report(QUALITY_TXT)
    
    # Print summary
    print("\n📋 Quality Analysis Summary:")
    quality_analyzer.print_summary()
    
    # Display first 10 rows of detailed report
    if not quality_report.empty:
        print("\n📊 Quality Report Preview (First 10 devices):")
        print(quality_report.head(10).to_string(index=False))
    
    print(f"✅ Quality analysis completed - reports saved")
    
except Exception as e:
    print(f"❌ Quality analysis failed: {e}")

# --- STEP 3: Trend Analysis ---
print("\n📈 STEP 3: TREND ANALYSIS")
print("-" * 30)

try:
    # Use FIXED DataTrendAnalyzer (filename-based, uses config dates)
    trend_analyzer = DataTrendAnalyzer(
        config_filename=CONFIG_FILE,
        csv_filename=OUTPUT_CSV,
        trend_threshold=10.0
    )
    
    # Run trend analysis
    trend_report = trend_analyzer.analyze_trends()
    
    # Save reports using FIXED methods
    trend_analyzer.save_report(TREND_CSV)
    trend_analyzer.save_text_report(TREND_TXT)
    
    # Print summary
    print("\n📋 Trend Analysis Summary:")
    trend_analyzer.print_summary()
    
    # Display first 10 rows of detailed report
    if not trend_report.empty:
        print("\n📊 Trend Report Preview (First 10 devices):")
        print(trend_report.head(10).to_string(index=False))
    else:
        print("📊 No significant trends detected")
    
    print(f"✅ Trend analysis completed - reports saved")
    
except Exception as e:
    print(f"❌ Trend analysis failed: {e}")

# --- STEP 4: Out-of-Hours Consumption Analysis ---
print("\n🕐 STEP 4: OUT-OF-HOURS CONSUMPTION ANALYSIS")
print("-" * 30)

try:
    # Use FIXED OutOfHoursConsumptionAnalyzer (filename-based, uses config dates)
    out_of_hours_analyzer = OutOfHoursConsumptionAnalyzer(
        config_filename=CONFIG_FILE,
        csv_filename=OUTPUT_CSV,
        out_of_hours_threshold=30.0
    )
    
    # Run out-of-hours analysis
    out_of_hours_report = out_of_hours_analyzer.analyze_consumption()
    
    # Save reports using FIXED methods
    out_of_hours_analyzer.save_report(OUT_OF_HOURS_CSV)
    out_of_hours_analyzer.save_text_report(OUT_OF_HOURS_TXT)
    
    # Print summary
    print("\n📋 Out-of-Hours Analysis Summary:")
    out_of_hours_analyzer.print_summary()
    
    # Display detailed results
    if not out_of_hours_report.empty:
        print("\n📊 Out-of-Hours Report Preview (First 10 flagged devices):")
        print(out_of_hours_report.head(10).to_string(index=False))
    else:
        print("📊 No out-of-hours consumption issues detected")
    
    print(f"✅ Out-of-hours analysis completed - reports saved")
    
except Exception as e:
    print(f"❌ Out-of-hours analysis failed: {e}")

# --- FINAL SUMMARY ---
print("\n" + "="*80)
print("🎉 HOURLY DATA ANALYSIS WORKFLOW COMPLETE")
print("="*80)
print("📋 Generated Reports:")
print(f"    📊 Extracted Data: outputs/{OUTPUT_CSV}")
print(f"    🔍 Quality Report (CSV): outputs/{QUALITY_CSV}")
print(f"    📋 Quality Report (Text): outputs/{QUALITY_TXT}")
print(f"    📈 Trend Report (CSV): outputs/{TREND_CSV}")
print(f"    📋 Trend Report (Text): outputs/{TREND_TXT}")
print(f"    🕐 Out-of-Hours Report (CSV): outputs/{OUT_OF_HOURS_CSV}")
print(f"    📋 Out-of-Hours Report (Text): outputs/{OUT_OF_HOURS_TXT}")
print("="*80)
