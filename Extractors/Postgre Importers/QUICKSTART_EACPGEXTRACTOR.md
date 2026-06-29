"""
EACpgExtractor - Quick Start Guide
===================================

WHAT IS THIS?
─────────────
EACpgExtractor.py is a scheduler that automatically runs the EAC data export
to PostgreSQL at midnight every day. It:

  ✓ Imports export_eac_data_to_postgresql from eacPgExport
  ✓ Runs an initial export on startup
  ✓ Waits until midnight
  ✓ Runs scheduled exports daily at midnight
  ✓ Logs all operations to console and file
  ✓ Handles errors gracefully (continues on failure)


FEATURES
────────
✅ Proper Import Handling
   - Error checking for all imports
   - Clear error messages if imports fail
   - Graceful exit on import failure

✅ Comprehensive Logging
   - Console output with timestamps
   - File logging to eac_pg_extractor.log
   - Detailed operation tracking

✅ Robust Error Handling
   - Try/catch blocks around exports
   - Continues on error (doesn't crash)
   - Full exception traceback logging

✅ Scheduled Execution
   - Waits until midnight
   - Runs export automatically
   - Loops indefinitely


QUICK START
───────────

Option 1: Run Once (One-Time Export)
────────────────────────────────────
Just run the script:

    cd "C:\Users\innoc\PycharmProjects\ETLScripts-main\Extractors\Postgre Importers"
    python EACpgExtractor.py

This will:
  1. Perform initial export immediately
  2. Wait until midnight
  3. Perform scheduled export
  4. Continue running indefinitely


Option 2: Run in Background (Windows)
──────────────────────────────────────
Create a batch file to run it in background:

    Create file: C:\start_eac_extractor.bat
    Content:
    ──────────
    @echo off
    cd C:\Users\innoc\PycharmProjects\ETLScripts-main\Extractors\Postgre Importers
    python EACpgExtractor.py

    Then run: start_eac_extractor.bat


Option 3: Windows Task Scheduler (Recommended)
───────────────────────────────────────────────
1. Open Task Scheduler
2. Create Basic Task
3. Set trigger: At startup (or specific time)
4. Set action: Run program
5. Program: python.exe (full path)
6. Arguments: C:\Users\innoc\PycharmProjects\ETLScripts-main\Extractors\Postgre Importers\EACpgExtractor.py


MONITORING
──────────
Check the logs:

    Real-time (PowerShell):
    Get-Content -Path "eac_pg_extractor.log" -Wait

    Recent entries:
    Get-Content -Path "eac_pg_extractor.log" -Tail 50

    Search for errors:
    Select-String "ERROR" eac_pg_extractor.log


LOG OUTPUT EXAMPLE
──────────────────
2026-06-19 12:30:00,123 - INFO - EAC PostgreSQL Extractor started at: 2026-06-19 12:30:00.123456
2026-06-19 12:30:00,234 - INFO - ✓ Successfully imported export_eac_data_to_postgresql
2026-06-19 12:30:00,345 - INFO - ✓ Successfully imported job scheduler
2026-06-19 12:30:00,456 - INFO - ================================================================================
2026-06-19 12:30:00,567 - INFO - PERFORMING INITIAL EAC DATA EXPORT
2026-06-19 12:30:00,678 - INFO - ================================================================================
2026-06-19 12:30:00,789 - INFO - Starting upsert process...
2026-06-19 12:30:05,890 - INFO - Initial export completed:
2026-06-19 12:30:05,901 - INFO -   ✓ Inserted: 30000
2026-06-19 12:30:05,912 - INFO -   ✓ Updated: 15000
2026-06-19 12:30:05,923 - INFO -   ✓ Skipped: 5000
2026-06-19 12:30:05,934 - INFO -   ✗ Errors: 0
2026-06-19 12:30:05,945 - INFO - ================================================================================
2026-06-19 12:30:05,956 - INFO - STARTING SCHEDULER - Waiting for midnight...
2026-06-19 12:30:05,967 - INFO - ================================================================================
2026-06-19 12:30:05,978 - INFO - ⏳ Time until next run: 11:29:55
    (countdown continues...)
2026-06-20 00:00:00,000 - INFO - 🔥 Midnight reached! Running export job...
2026-06-20 00:00:00,111 - INFO - ================================================================================
2026-06-20 00:00:00,222 - INFO - SCHEDULED JOB STARTED at 2026-06-20 00:00:00.222222
2026-06-20 00:00:00,333 - INFO - ================================================================================
...


TROUBLESHOOTING
───────────────

Problem: "ImportError: Failed to import export_eac_data_to_postgresql"
Fix:
  1. Verify eacPgExport.py exists in: etl/Postgres Export/
  2. Check dao/postgresquarterupsert.py exists
  3. Verify all module files are present
  4. Check PYTHONPATH includes project root


Problem: "ImportError: Failed to import job scheduler"
Fix:
  1. Verify legacy/runSchedular.py exists
  2. Check legacy/__init__.py exists (if needed)
  3. Verify file names match exactly (case-sensitive)


Problem: "No module named 'etl'"
Fix:
  1. Make sure you're running from the project root
  2. Check sys.path.insert line is correct
  3. Verify __init__.py exists in etl/ directory


Problem: "ModuleNotFoundError: No module named 'Postgres Export'"
Fix:
  1. The space in "Postgres Export" is handled correctly
  2. Verify directory name has exactly this spelling
  3. If issues persist, rename directory to "postgres_export" (no space)


WHAT HAPPENS IF EXPORT FAILS?
───────────────────────────────
If the export fails:
  ✓ Error is logged with full traceback
  ✓ Script continues running (doesn't crash)
  ✓ Waits for next midnight
  ✓ Retries at next scheduled time


CUSTOMIZING THE SCHEDULE
───────────────────────────
To run at a different time instead of midnight:

1. Modify the countdown_to_midnight() function to use a different time
2. Or modify the main loop to use a different scheduler like APScheduler

Example (run at 2 AM instead):
    # In the main loop, calculate seconds until 2 AM instead of midnight


DATABASE REQUIREMENTS
──────────────────────
✓ PostgreSQL table: eac_quarterly_data (or custom name)
✓ Composite unique constraint: UNIQUE(patientuuid, quarter)
✓ Required columns: PatientUUID, Quarter, touchtime
✓ MongoDB connection configured in dao/config.py
✓ PostgreSQL connection configured in dao/config.py


FILES INVOLVED
───────────────
✓ Extractors/Postgre Importers/EACpgExtractor.py (THIS FILE - scheduler)
✓ etl/Postgres Export/eacPgExport.py (exports data)
✓ dao/postgresquarterupsert.py (upsert logic)
✓ eac_pg_extractor.log (output log file)


ADVANCED USAGE
───────────────

To run with custom parameters, modify the file before running:

Instead of:
    export_eac_data_to_postgresql(cutoff_datetime=None)

Use:
    from datetime import datetime
    export_eac_data_to_postgresql(
        cutoff_datetime=datetime(2024, 1, 1),
        table_name="custom_table_name",
        batch_size=10000,
        filter_aspire_only=True
    )


PERFORMANCE NOTES
──────────────────
Runtime depends on:
  - Number of EAC containers in MongoDB (~1-5 mins for 50K records)
  - PostgreSQL write performance
  - Network latency
  - Batch size (larger batches = faster)

Monitor the logs to track performance.


STATUS
──────
✅ Ready to run
✅ All imports configured
✅ Error handling included
✅ Logging configured
✅ Scheduler loop ready

Just execute: python EACpgExtractor.py
"""

print(__doc__)

