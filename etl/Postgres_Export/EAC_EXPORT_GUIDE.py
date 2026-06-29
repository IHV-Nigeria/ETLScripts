"""
EAC PostgreSQL Export Integration Guide
========================================

This script integrates the Quarter-Based Upsert system with your existing
EACDataExportMultiprocess.py workflow.

WHAT WAS ADDED:
===============

1. Quarter-Based Upsert Integration
   - Automatic insert/update/skip logic based on patientuuid and quarter
   - Touchtime comparison prevents data loss
   - Batch processing for efficiency

2. Main Execution Function: export_eac_data_to_postgresql()
   - Connects to MongoDB and extracts EAC containers
   - Loads facility cache for fast lookups
   - Processes records in batches (default 5000)
   - Uperts to PostgreSQL with detailed tracking
   - Logs all operations with timestamps

3. Helper Function: _upsert_batch()
   - Handles batch upsert operations
   - Error handling and logging
   - Returns operation statistics

4. Main Entry Point
   - Can be run directly: python eacPgExport.py
   - Or imported and called from other scripts


QUICK START:
============

Option 1: Run Directly (Default Settings)
──────────────────────────────────────────

cd C:\\Users\\innoc\\PycharmProjects\\ETLScripts-main\\etl\\Postgres Export
python eacPgExport.py

This will:
  ✓ Extract all EAC containers from MongoDB
  ✓ Process records in batches of 5000
  ✓ Upsert to eac_quarterly_data table
  ✓ Log results to eac_pg_export.log


Option 2: Import and Call from Another Script
───────────────────────────────────────────────

from etl.Postgres_Export.eacPgExport import export_eac_data_to_postgresql
from datetime import datetime

result = export_eac_data_to_postgresql(
    cutoff_datetime=datetime(2024, 1, 1),
    table_name="eac_quarterly_data",
    batch_size=5000,
    filter_aspire_only=False
)

print(f"Inserted: {result['inserted']}")
print(f"Updated: {result['updated']}")
print(f"Skipped: {result['skipped']}")


FUNCTION SIGNATURE:
===================

export_eac_data_to_postgresql(
    cutoff_datetime=None,           # datetime object (default: now)
    table_name="eac_quarterly_data",  # PostgreSQL table name
    batch_size=5000,                 # Records per batch
    filter_aspire_only=False         # Only ASPIRE states (FCT, KATSINA, NASARAWA, RIVERS)
)

Returns:
--------
{
    "total_processed": int,   # Total records processed
    "inserted": int,          # New records inserted
    "updated": int,           # Existing records updated
    "skipped": int,           # Records skipped (same quarter, newer touchtime)
    "errors": int             # Records with errors
}


PARAMETERS EXPLAINED:
=====================

1. cutoff_datetime: datetime object
   Purpose: Only extract data up to this date
   Default: None (uses current datetime)
   
   Examples:
     cutoff_datetime=None              # Use now
     cutoff_datetime=datetime.now()    # Same as above
     cutoff_datetime=datetime(2024, 1, 1)  # Q1 2024 cutoff
     cutoff_datetime=datetime(2025, 12, 31)  # End of 2025


2. table_name: str
   Purpose: PostgreSQL table name for inserts
   Default: "eac_quarterly_data"
   
   Make sure this table exists with:
     ✓ Columns matching record fields (PatientUUID, Quarter, touchtime, etc.)
     ✓ Composite unique constraint: UNIQUE(patientuuid, quarter)
     ✓ Recommended indexes (see SCHEMA_SETUP.sql)


3. batch_size: int
   Purpose: Number of records to process before upserting
   Default: 5000
   
   Recommendations:
     100-500    → For testing
     1000-3000  → Standard production
     5000-10000 → Large environments
     Note: Larger batches = faster but more memory


4. filter_aspire_only: bool
   Purpose: If True, only process facilities in ASPIRE states
   Default: False
   
   ASPIRE States:
     - FCT
     - KATSINA
     - NASARAWA
     - RIVERS
   
   Examples:
     filter_aspire_only=False   # All states
     filter_aspire_only=True    # ASPIRE states only


USAGE EXAMPLES:
===============

Example 1: Standard Export (All States, Current Date)
──────────────────────────────────────────────────────

from etl.Postgres_Export.eacPgExport import export_eac_data_to_postgresql

result = export_eac_data_to_postgresql()

print(f"Export Complete:")
print(f"  Inserted: {result['inserted']}")
print(f"  Updated: {result['updated']}")
print(f"  Skipped: {result['skipped']}")


Example 2: ASPIRE States Only with Custom Batch Size
─────────────────────────────────────────────────────

result = export_eac_data_to_postgresql(
    filter_aspire_only=True,
    batch_size=10000
)

# Result tracking
if result['errors'] == 0:
    print("✓ Export successful with no errors")
else:
    print(f"⚠ Export completed with {result['errors']} errors")


Example 3: Specific Date Range (Q1 2024)
─────────────────────────────────────────

from datetime import datetime

# Export up to end of Q1 2024
result = export_eac_data_to_postgresql(
    cutoff_datetime=datetime(2024, 3, 31),
    table_name="eac_quarterly_data_q1_2024",
    batch_size=3000
)


Example 4: Integration with Existing ETL Pipeline
──────────────────────────────────────────────────

# In your existing ETL script
import logging
from etl.Postgres_Export.eacPgExport import export_eac_data_to_postgresql

logger = logging.getLogger(__name__)

try:
    logger.info("Starting EAC export...")
    result = export_eac_data_to_postgresql(
        batch_size=5000
    )
    
    logger.info(f"Inserted: {result['inserted']}, Updated: {result['updated']}")
    
    if result['errors'] > 0:
        logger.warning(f"Completed with {result['errors']} errors")
        # Send alert or notification
    
except Exception as e:
    logger.error(f"Export failed: {e}")
    raise


UPSERT LOGIC EXPLAINED:
=======================

The Quarter-Based Upsert Logic:
────────────────────────────────

1. Record arrives from MongoDB with:
   - PatientUUID (unique patient ID)
   - Quarter (e.g., Q1-2024, Q2-2024)
   - touchtime (timestamp of last modification)


2. System checks: Does (PatientUUID + Quarter) exist in PostgreSQL?

   YES (exists):
   └─ Compare touchtime values:
      ├─ New touchtime > DB touchtime? → UPDATE entire record
      └─ New touchtime <= DB touchtime? → SKIP (keep existing data)

   NO (doesn't exist):
   └─ INSERT new record


3. Result Statistics:
   - inserted: Number of new records added
   - updated: Number of existing records updated
   - skipped: Number of records NOT updated (DB data is newer)
   - errors: Number of records with processing errors


EXAMPLE SCENARIOS:
══════════════════

Scenario 1: First Ever Record
─────────────────────────────
Patient: ABC-123, Quarter: Q1-2024
Status in DB: NOT FOUND
Action: INSERT
Result: +1 inserted


Scenario 2: Same Patient, Same Quarter, Newer Data
───────────────────────────────────────────────────
Patient: ABC-123, Quarter: Q1-2024
Status in DB: FOUND with touchtime 2024-01-15 10:00:00
New Record touchtime: 2024-01-16 14:30:00 (newer)
Action: UPDATE
Result: +1 updated, -1 skipped


Scenario 3: Same Patient, Same Quarter, Older Data
──────────────────────────────────────────────────
Patient: ABC-123, Quarter: Q1-2024
Status in DB: FOUND with touchtime 2024-01-16 14:30:00
New Record touchtime: 2024-01-15 10:00:00 (older)
Action: SKIP (don't overwrite newer data)
Result: +1 skipped


Scenario 4: Same Patient, Different Quarter
─────────────────────────────────────────────
Patient: ABC-123, Quarter: Q2-2024
Status in DB for Q2: NOT FOUND
Action: INSERT (separate entry for different quarter)
Result: +1 inserted


LOGGING OUTPUT:
===============

The script logs detailed information to:
  File: eac_pg_export.log
  Console: Real-time progress with TQDM progress bar

Sample Log Output:
─────────────────
2026-06-19 11:30:45,123 - INFO - Starting EAC data export to PostgreSQL
2026-06-19 11:30:45,234 - INFO - Cutoff datetime: 2026-06-19 11:30:45.123456
2026-06-19 11:30:45,345 - INFO - Target table: eac_quarterly_data
2026-06-19 11:30:45,456 - INFO - Batch size: 5000
2026-06-19 11:30:45,567 - INFO - Filter ASPIRE only: False
2026-06-19 11:30:45,678 - INFO - Connecting to MongoDB...
2026-06-19 11:30:46,789 - INFO - Loading facility cache...
2026-06-19 11:30:46,890 - INFO - Loaded 500 facilities into memory cache.
2026-06-19 11:30:46,901 - INFO - Connecting to PostgreSQL...
2026-06-19 11:30:46,912 - INFO - Fetching EAC containers from MongoDB...
2026-06-19 11:30:47,023 - INFO - Found 50000 total EAC containers
2026-06-19 11:30:47,134 - INFO - Processing records...
EAC Export Progress: 100%|████████| 50000/50000 [05:23<00:00, 154.67it/s]
2026-06-19 11:35:50,123 - INFO - Upserting batch 1 (5000 records) to eac_quarterly_data...
2026-06-19 11:35:52,234 - INFO - Batch 1 result: 3000 inserted, 1500 updated, 500 skipped
...
2026-06-19 11:40:30,123 - INFO - ======================================================================
2026-06-19 11:40:30,234 - INFO - EAC EXPORT COMPLETED
2026-06-19 11:40:30,345 - INFO - ======================================================================
2026-06-19 11:40:30,456 - INFO - Total records processed: 50000
2026-06-19 11:40:30,567 - INFO -   ✓ Inserted: 30000
2026-06-19 11:40:30,678 - INFO -   ✓ Updated: 15000
2026-06-19 11:40:30,789 - INFO -   ✓ Skipped: 5000
2026-06-19 11:40:30,890 - INFO -   ✗ Errors: 0
2026-06-19 11:40:30,901 - INFO - Success rate: 90.0%
2026-06-19 11:40:30,912 - INFO - ======================================================================
2026-06-19 11:40:31,023 - INFO - PostgreSQL connection closed


DATABASE TABLE SETUP:
====================

Before running the export, ensure your PostgreSQL table exists:

CREATE TABLE eac_quarterly_data (
    recordid SERIAL PRIMARY KEY,
    PatientUUID VARCHAR(100) NOT NULL,
    Quarter VARCHAR(10) NOT NULL,
    touchtime TIMESTAMP,
    DatimCode VARCHAR(50),
    FacilityName VARCHAR(255),
    State VARCHAR(100),
    LGA VARCHAR(100),
    UniqueID VARCHAR(100),
    HospitalNumber VARCHAR(100),
    Sex VARCHAR(10),
    -- ... add all other EAC fields from convert_doc_to_record() ...
    
    -- CRITICAL: Composite unique constraint
    UNIQUE(PatientUUID, Quarter)
);

-- Recommended Indexes
CREATE INDEX idx_patientuuid_quarter 
ON eac_quarterly_data(PatientUUID, Quarter);

CREATE INDEX idx_touchtime 
ON eac_quarterly_data(touchtime);

CREATE INDEX idx_quarter 
ON eac_quarterly_data(Quarter);


TROUBLESHOOTING:
================

Problem: "UNIQUE constraint violated"
──────────────────────────────────────
Cause: Table missing UNIQUE(PatientUUID, Quarter) constraint
Fix: Add the constraint to your table


Problem: Records not updating
─────────────────────────────
Cause: New records have older touchtime than DB records
Fix: Check touchtime values and ensure they're set correctly
     Verify timezone consistency


Problem: Slow performance
────────────────────────
Cause: Missing indexes or large batch size
Fix: 1. Create recommended indexes (see SCHEMA_SETUP.sql)
     2. Reduce batch_size (try 3000)
     3. Run VACUUM ANALYZE on table:
        VACUUM ANALYZE eac_quarterly_data;


Problem: Connection timeout
──────────────────────────
Cause: Database unreachable
Fix: 1. Verify PostgreSQL is running
     2. Check config.py (DB_HOST, DB_PORT, DB_USER, DB_PASS)
     3. Test connection: python quickstart_demo.py


PERFORMANCE TIPS:
================

Batch Size Optimization:
  - Smaller batches (1000): More frequent commits, slower overall
  - Larger batches (10000): Fewer commits, faster, uses more memory
  - Recommended: 3000-5000 for most cases

Memory Usage:
  - Each batch is processed independently
  - Memory is freed after each batch upsert
  - Suitable for large-scale operations

Throughput:
  - Expected: 1000-5000 records/second
  - Depends on: Hardware, network, database performance
  - Monitor: Check eac_pg_export.log for actual throughput


INTEGRATION WITH SCHEDULER:
===========================

Windows Task Scheduler
──────────────────────

Create batch file: C:\\eac_export.bat
────────────────────────────────────
@echo off
cd C:\\Users\\innoc\\PycharmProjects\\ETLScripts-main
python etl\\Postgres Export\\eacPgExport.py >> C:\\eac_export_%date:~-4,4%%date:~-10,2%%date:~-7,2%.log 2>&1

Then schedule the batch file to run daily


Python Schedule
───────────────

Create scheduler script:
──────────────────────
import schedule
import time
from etl.Postgres_Export.eacPgExport import export_eac_data_to_postgresql

# Run export daily at 2 AM
schedule.every().day.at("02:00").do(export_eac_data_to_postgresql)

while True:
    schedule.run_pending()
    time.sleep(60)


NEXT STEPS:
===========

1. ✓ Verify PostgreSQL table exists (use SCHEMA_SETUP.sql)
2. ✓ Test the export: python eacPgExport.py
3. ✓ Check logs: tail -f eac_pg_export.log
4. ✓ Verify data in PostgreSQL: SELECT * FROM eac_quarterly_data LIMIT 10
5. ✓ Set up scheduling for automated runs
6. ✓ Monitor performance and adjust batch_size as needed


FOR HELP:
=========

See these files for additional information:
  - dao/postgresquarterupsert.py - Main upsert module
  - QUARTER_UPSERT_README.md - Complete API documentation
  - SCHEMA_SETUP.sql - Database setup and maintenance
  - INTEGRATION_GUIDE.py - Integration patterns
  - QUICK_REFERENCE.py - Quick lookup guide
"""

print(__doc__)

