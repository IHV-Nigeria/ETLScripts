"""
EAC PostgreSQL Export - Quick Reference
========================================

Modified: eacPgExport.py

NEW ADDITIONS:
  ✓ import dao.postgresquarterupsert as postgres_upsert
  ✓ export_eac_data_to_postgresql() - Main export function
  ✓ _upsert_batch() - Batch upsert helper
  ✓ if __name__ == "__main__": - Main entry point


SYNTAX REFERENCE:
=================

┌─────────────────────────────────────────────────────────────┐
│ BASIC USAGE                                                 │
└─────────────────────────────────────────────────────────────┘

# Option 1: Direct execution (simplest)
python eacPgExport.py

# Option 2: Import and call
from etl.Postgres_Export.eacPgExport import export_eac_data_to_postgresql
result = export_eac_data_to_postgresql()


┌─────────────────────────────────────────────────────────────┐
│ FUNCTION SIGNATURE                                          │
└─────────────────────────────────────────────────────────────┘

export_eac_data_to_postgresql(
    cutoff_datetime=None,              # Default: now
    table_name="eac_quarterly_data",   # Default: eac_quarterly_data
    batch_size=5000,                   # Default: 5000
    filter_aspire_only=False           # Default: False
)

Returns:
{
    "total_processed": int,    # Total records processed
    "inserted": int,           # New records inserted
    "updated": int,            # Existing records updated
    "skipped": int,            # Records skipped
    "errors": int              # Records with errors
}


┌─────────────────────────────────────────────────────────────┐
│ COMMON USAGE PATTERNS                                       │
└─────────────────────────────────────────────────────────────┘

Pattern 1: Run with all defaults
──────────────────────────────────
export_eac_data_to_postgresql()

Pattern 2: Custom date range (Q1 2024)
───────────────────────────────────────
from datetime import datetime

export_eac_data_to_postgresql(
    cutoff_datetime=datetime(2024, 3, 31)
)

Pattern 3: ASPIRE states only with custom batch size
─────────────────────────────────────────────────────
export_eac_data_to_postgresql(
    filter_aspire_only=True,
    batch_size=10000
)

Pattern 4: Custom table name and all options
────────────────────────────────────────────
export_eac_data_to_postgresql(
    cutoff_datetime=datetime(2024, 1, 1),
    table_name="eac_export_test",
    batch_size=3000,
    filter_aspire_only=False
)

Pattern 5: With error handling
───────────────────────────────
try:
    result = export_eac_data_to_postgresql()
    
    if result['errors'] > 0:
        print(f"⚠ Completed with {result['errors']} errors")
    else:
        print("✓ Export successful")
        
except Exception as e:
    print(f"✗ Export failed: {e}")


┌─────────────────────────────────────────────────────────────┐
│ PARAMETER REFERENCE                                         │
└─────────────────────────────────────────────────────────────┘

cutoff_datetime
  Type: datetime.datetime or None
  Default: None (uses current datetime)
  
  Purpose: Cutoff date for data extraction
  
  Examples:
    None                         # Now
    datetime.now()              # Now
    datetime(2024, 1, 1)        # Jan 1, 2024
    datetime(2024, 3, 31)       # Mar 31, 2024 (end of Q1)
    datetime(2024, 12, 31)      # Dec 31, 2024 (end of year)

────────────────────────────────────────────────────────────

table_name
  Type: str
  Default: "eac_quarterly_data"
  
  Purpose: PostgreSQL table name for inserts
  
  Examples:
    "eac_quarterly_data"        # Production table
    "eac_export_test"           # Test table
    "eac_aspire_only"           # ASPIRE data only table

────────────────────────────────────────────────────────────

batch_size
  Type: int
  Default: 5000
  
  Purpose: Records to process before upserting
  
  Recommendations:
    100    → Debug/testing, slow
    1000   → Small exports
    5000   → Standard (default)
    10000  → Large exports, more memory

────────────────────────────────────────────────────────────

filter_aspire_only
  Type: bool
  Default: False
  
  Purpose: Filter by ASPIRE states (FCT, KATSINA, NASARAWA, RIVERS)
  
  When True:
    ✓ Only processes ASPIRE state facilities
    ✓ All other states are skipped
    ✓ Useful for targeted exports
    
  When False:
    ✓ Processes all facilities
    ✓ No state filtering applied


┌─────────────────────────────────────────────────────────────┐
│ RETURN VALUE INTERPRETATION                                 │
└─────────────────────────────────────────────────────────────┘

Example Result:
{
    "total_processed": 50000,
    "inserted": 30000,
    "updated": 15000,
    "skipped": 5000,
    "errors": 0
}

Meaning:
  ✓ 50,000 total records processed
  ✓ 30,000 new records inserted into PostgreSQL
  ✓ 15,000 existing records updated (newer data)
  ✓ 5,000 records skipped (DB data was newer)
  ✓ 0 records had errors

Success Rate = (inserted + updated) / total_processed
             = (30000 + 15000) / 50000 = 90%


┌─────────────────────────────────────────────────────────────┐
│ FLOW DIAGRAM                                                │
└─────────────────────────────────────────────────────────────┘

export_eac_data_to_postgresql()
    ↓
    1. Connect to MongoDB
    ↓
    2. Load facility cache (O(1) lookups)
    ↓
    3. Connect to PostgreSQL
    ↓
    4. Fetch EAC containers from MongoDB
    ↓
    5. For each document:
       └─ convert_doc_to_record()
       └─ Add to batch
       └─ When batch >= batch_size:
          └─ _upsert_batch()
             └─ batch_upsert_by_quarter()
                └─ INSERT/UPDATE/SKIP logic
    ↓
    6. Upsert remaining records
    ↓
    7. Close connections and return stats


┌─────────────────────────────────────────────────────────────┐
│ UPSERT LOGIC                                                │
└─────────────────────────────────────────────────────────────┘

For each record:

1. Composite Key Check: (PatientUUID, Quarter)
   
   NOT FOUND in DB
   └─ ACTION: INSERT
      └─ Result: +1 inserted
   
   FOUND in DB
   └─ Compare touchtime:
      ├─ New touchtime > DB touchtime
      │  └─ ACTION: UPDATE
      │     └─ Result: +1 updated
      │
      └─ New touchtime <= DB touchtime
         └─ ACTION: SKIP
            └─ Result: +1 skipped


┌─────────────────────────────────────────────────────────────┐
│ LOGGING                                                     │
└─────────────────────────────────────────────────────────────┘

Log File: eac_pg_export.log (in current directory)

Log Levels:
  INFO: Execution progress and statistics
  ERROR: Errors during processing
  DEBUG: Detailed diagnostic info

Example Log Entry:
2026-06-19 11:30:45,123 - INFO - Batch 1 result: 3000 inserted, 1500 updated, 500 skipped

Monitor Log in Real-Time:
Linux/Mac:     tail -f eac_pg_export.log
Windows:       Get-Content -Path eac_pg_export.log -Wait


┌─────────────────────────────────────────────────────────────┐
│ PREREQUISITES                                               │
└─────────────────────────────────────────────────────────────┘

✓ MongoDB connection configured (dao/config.py)
✓ PostgreSQL connection configured (dao/config.py)
✓ PostgreSQL table created with:
  ├─ UNIQUE(patientuuid, quarter) constraint
  ├─ All required columns from convert_doc_to_record()
  └─ Recommended indexes on patientuuid, quarter, touchtime
✓ Facility cache loaded for fast lookups


┌─────────────────────────────────────────────────────────────┐
│ ERROR HANDLING                                              │
└─────────────────────────────────────────────────────────────┘

try/except block:

try:
    result = export_eac_data_to_postgresql()
    
    # Check for errors
    if result['errors'] > 0:
        print(f"Export completed with {result['errors']} errors")
        # Handle partial failure
    
    # Check success rate
    total = sum([result['inserted'], result['updated']])
    success_rate = total / result['total_processed'] * 100
    
    if success_rate < 90:
        print(f"Warning: Low success rate ({success_rate:.1f}%)")

except KeyboardInterrupt:
    print("Export cancelled")

except Exception as e:
    print(f"Fatal error: {e}")
    import traceback
    traceback.print_exc()


┌─────────────────────────────────────────────────────────────┐
│ PERFORMANCE TUNING                                          │
└─────────────────────────────────────────────────────────────┘

Factor 1: Batch Size
  Smaller (1000):  More frequent commits, slower total
  Larger (10000):  Fewer commits, faster, requires more memory
  Sweet Spot:      3000-5000

Factor 2: Indexes
  Must have: UNIQUE(patientuuid, quarter)
  Should have: INDEX on touchtime
  Result: 10-50x faster lookups

Factor 3: Filter
  filter_aspire_only=True reduces volume by ~25%
  May improve performance for targeted exports


┌─────────────────────────────────────────────────────────────┐
│ TROUBLESHOOTING                                             │
└─────────────────────────────────────────────────────────────┘

Problem: "Module not found: postgresquarterupsert"
Fix: Ensure dao/postgresquarterupsert.py exists

Problem: "No records processed"
Fix: 1. Check MongoDB connection (test with mongo_dao directly)
     2. Verify facility cache loads
     3. Check cutoff_datetime isn't filtering everything

Problem: "PostgreSQL connection failed"
Fix: 1. Check dao/config.py credentials
     2. Verify PostgreSQL is running
     3. Test with quickstart_demo.py

Problem: "UNIQUE constraint violation"
Fix: 1. Ensure table has: UNIQUE(patientuuid, quarter)
     2. Check for duplicate entries in existing table
     3. Verify column name case matches (PatientUUID vs patientuuid)

Problem: "Slow performance"
Fix: 1. Create recommended indexes
     2. Increase batch_size
     3. Run VACUUM ANALYZE on table
     4. Check database server performance


┌─────────────────────────────────────────────────────────────┐
│ NEXT STEPS                                                  │
└─────────────────────────────────────────────────────────────┘

1. Test locally:
   python eacPgExport.py

2. Monitor logs:
   tail -f eac_pg_export.log

3. Verify data:
   SELECT COUNT(*) FROM eac_quarterly_data;
   SELECT * FROM eac_quarterly_data LIMIT 10;

4. Schedule automated runs:
   Windows: Task Scheduler
   Linux: cron job

5. Set up monitoring/alerts:
   Check daily logs
   Track insert/update/skip ratios
   Alert on errors


┌─────────────────────────────────────────────────────────────┐
│ HELP & DOCUMENTATION                                        │
└─────────────────────────────────────────────────────────────┘

For more information, see:
  EAC_EXPORT_GUIDE.py - Comprehensive guide
  dao/postgresquarterupsert.py - Upsert module
  QUARTER_UPSERT_README.md - Full API docs
  SCHEMA_SETUP.sql - Database setup
  eacPgExport.py - Source code with comments
"""

print(__doc__)

