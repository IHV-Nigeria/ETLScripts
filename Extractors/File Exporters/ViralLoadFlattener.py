from datetime import datetime
import sys
import os

# Add parent directory to path so imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

import etl.CSV_FIle_Exporters.VLSuppression_Nas as VLSuppression_Nas
from etl.CSV_FIle_Exporters.ViralLoadExtractionFlattener import export_data

# Basic usage - extract all viral load records
# export_data()

# Extract with date range filter
# export_data(start_date=datetime(2023, 1, 1), end_date=datetime(2024, 12, 31))

# Extract with custom filename and date range
# export_data(start_date=datetime(2023, 6, 1), end_date=datetime(2024, 6, 30), filename="vl_2023_2024")

# Extract all records (no date filter)
export_data()
