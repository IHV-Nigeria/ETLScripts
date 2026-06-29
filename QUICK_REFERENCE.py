"""
QUICK REFERENCE CARD
PostgreSQL Quarter-Based Upsert System
"""

QUICK_REFERENCE = """
╔═══════════════════════════════════════════════════════════════════════════╗
║                   POSTGRESQL QUARTER UPSERT                              ║
║                        QUICK REFERENCE CARD                              ║
╚═══════════════════════════════════════════════════════════════════════════╝


───────────────────────────────────────────────────────────────────────────
BASIC USAGE - 3 LINES OF CODE
───────────────────────────────────────────────────────────────────────────

from dao import postgresdao, postgresquarterupsert
from datetime import datetime
conn = postgresdao.connect_to_postgresqldb()

records = [{
    'patientuuid': 'patient-001',
    'quarter': 'Q1-2024',
    'touchtime': datetime.now(),
    'field1': 'value1'
}]

result = postgresquarterupsert.batch_upsert_by_quarter(conn, 'table_name', records)
conn.close()
# Result: {"inserted": 1, "updated": 0, "skipped": 0}


───────────────────────────────────────────────────────────────────────────
IMPORT
───────────────────────────────────────────────────────────────────────────

from dao import postgresdao
from dao import postgresquarterupsert
from datetime import datetime


───────────────────────────────────────────────────────────────────────────
CONNECT TO DATABASE
───────────────────────────────────────────────────────────────────────────

conn = postgresdao.connect_to_postgresqldb()
if not conn:
    print("Connection failed!")
    raise Exception("Cannot connect to database")


───────────────────────────────────────────────────────────────────────────
BATCH UPSERT (RECOMMENDED FOR 10+ RECORDS)
───────────────────────────────────────────────────────────────────────────

result = postgresquarterupsert.batch_upsert_by_quarter(
    conn,                           # PostgreSQL connection
    'my_table',                     # Table name
    records,                        # List of dicts
    protected_keys={'recordid', 'patientuuid', 'quarter'}  # Optional
)

# Returns: {"inserted": int, "updated": int, "skipped": int}
# Example: {"inserted": 100, "updated": 50, "skipped": 25}


───────────────────────────────────────────────────────────────────────────
SINGLE RECORD UPSERT
───────────────────────────────────────────────────────────────────────────

result = postgresquarterupsert.compare_and_upsert_record(
    conn,
    'my_table',
    single_record
)

# Returns: {"action": "inserted|updated|skipped", "details": str}
# Example: {"action": "updated", "details": "Record updated for abc-123/Q1-2024"}


───────────────────────────────────────────────────────────────────────────
PRE-CHECK EXISTING RECORDS
───────────────────────────────────────────────────────────────────────────

existing = postgresquarterupsert.get_existing_quarter_records(
    conn,
    'my_table',
    ['patient-001', 'patient-002']
)

# Return: {(uuid, quarter): {'touchtime': datetime}, ...}
# Usage: if ('patient-001', 'Q1-2024') in existing:


───────────────────────────────────────────────────────────────────────────
RECORD STRUCTURE (REQUIRED FIELDS)
───────────────────────────────────────────────────────────────────────────

record = {
    'patientuuid': 'unique-id',           # REQUIRED: Patient identifier
    'quarter': 'Q1-2024',                 # REQUIRED: Format Q[1-4]-[YYYY]
    'touchtime': datetime.now(),          # REQUIRED: Last modification time
    'field1': 'value1',                   # Optional: Any other fields
    'field2': 'value2',
    # ... more fields ...
}

Required fields:
  - patientuuid (string) - Unique patient ID
  - quarter (string) - Format: Q1-2024, Q2-2024, Q3-2024, Q4-2024
  - touchtime (datetime) - Timestamp of last modification


───────────────────────────────────────────────────────────────────────────
QUARTER CALCULATION
───────────────────────────────────────────────────────────────────────────

from datetime import datetime

date = datetime.now()
quarter = f"Q{(date.month - 1) // 3 + 1}-{date.year}"
# Example result: Q2-2026


───────────────────────────────────────────────────────────────────────────
LOGIC SUMMARY
───────────────────────────────────────────────────────────────────────────

RECORD EXISTS?           TOUCHTIME COMPARISON        ACTION
─────────────────────────────────────────────────────────────
NO                       N/A                         INSERT
YES (same uuid+quarter)  New > DB touchtime          UPDATE
YES (same uuid+quarter)  New <= DB touchtime         SKIP
Different quarter        N/A                         INSERT


───────────────────────────────────────────────────────────────────────────
UTILITY FUNCTIONS
───────────────────────────────────────────────────────────────────────────

# Get records by quarter range
records = postgresquarterupsert.get_records_by_quarter_range(
    conn, 'table', 'Q1-2024', 'Q4-2024', limit=1000
)

# Get touchtime by key pairs
touchtimes = postgresquarterupsert.get_existing_touchtimes_by_quarter(
    conn, 'table', [('uuid1', 'Q1-2024'), ('uuid2', 'Q1-2024')]
)

# Delete a record
postgresquarterupsert.delete_records_by_quarter(
    conn, 'table', 'uuid', 'Q1-2024'
)


───────────────────────────────────────────────────────────────────────────
DATABASE REQUIREMENTS
───────────────────────────────────────────────────────────────────────────

CREATE TABLE my_table (
    recordid SERIAL PRIMARY KEY,
    patientuuid VARCHAR(100) NOT NULL,
    quarter VARCHAR(10) NOT NULL,
    touchtime TIMESTAMP,
    -- your columns here --
    UNIQUE(patientuuid, quarter)  ← MUST HAVE THIS!
);

CREATE INDEX idx_patientuuid_quarter ON my_table(patientuuid, quarter);


───────────────────────────────────────────────────────────────────────────
BATCH SIZE RECOMMENDATIONS
───────────────────────────────────────────────────────────────────────────

Records     Use Case                Recommended
───────────────────────────────────────────────
< 100       Testing, debug          Direct call
100-500     Small daily export      1 batch
500-5000    Typical daily export    1-10 batches
5K-100K     Large weekly export     10-100 batches
100K+       Massive import          Split into 5K chunks


───────────────────────────────────────────────────────────────────────────
EXAMPLE: PROCESSING LARGE EXPORT
───────────────────────────────────────────────────────────────────────────

all_records = extract_from_mongo()  # Get 50,000 records

total_inserted = 0
total_updated = 0
total_skipped = 0

# Process in 5,000 record batches
for i in range(0, len(all_records), 5000):
    batch = all_records[i:i+5000]
    
    result = postgresquarterupsert.batch_upsert_by_quarter(
        conn, 'my_table', batch
    )
    
    total_inserted += result['inserted']
    total_updated += result['updated']
    total_skipped += result['skipped']
    
    print(f"Batch {i//5000 + 1}: {result}")

print(f"\\nFinal: {total_inserted} inserted, {total_updated} updated, {total_skipped} skipped")


───────────────────────────────────────────────────────────────────────────
CLOSE CONNECTION
───────────────────────────────────────────────────────────────────────────

if conn:
    conn.close()


───────────────────────────────────────────────────────────────────────────
ERROR HANDLING
───────────────────────────────────────────────────────────────────────────

try:
    result = postgresquarterupsert.batch_upsert_by_quarter(conn, table, records)
    print(f"Success: {result['inserted']} inserted, {result['updated']} updated")
    
except Exception as e:
    print(f"Error: {e}")
    # Check postgresql_errors.log for details


───────────────────────────────────────────────────────────────────────────
VERIFY SETUP
───────────────────────────────────────────────────────────────────────────

Run: python quickstart_demo.py

Expected output:
  ✓ Connection successful!
  ✓ Module imports working correctly
  ✓ Test table created successfully!
  ✓ Demo operations completed
  ✓ SETUP COMPLETE AND VALIDATED!


───────────────────────────────────────────────────────────────────────────
FILES LOCATION
───────────────────────────────────────────────────────────────────────────

Main module:           dao/postgresquarterupsert.py
Documentation:         QUARTER_UPSERT_README.md
Database schema:       SCHEMA_SETUP.sql
Integration guide:     INTEGRATION_GUIDE.py
Usage examples:        examples_quarter_upsert.py
Validation script:     quickstart_demo.py
This reference:        QUICK_REFERENCE.txt


───────────────────────────────────────────────────────────────────────────
COMMON PATTERNS
───────────────────────────────────────────────────────────────────────────

Pattern 1: MongoDB → PostgreSQL ETL
    records = extract_from_mongo()
    records_with_quarter = add_quarter_field(records)
    result = batch_upsert_by_quarter(conn, table, records_with_quarter)

Pattern 2: Pre-check before processing
    existing = get_existing_quarter_records(conn, table, uuids)
    to_process = [r for r in records if (r['uuid'], r['quarter']) not in existing]
    result = batch_upsert_by_quarter(conn, table, to_process)

Pattern 3: Incremental updates
    new_records = fetch_since_last_run()
    result = batch_upsert_by_quarter(conn, table, new_records)

Pattern 4: Single record comparison
    result = compare_and_upsert_record(conn, table, record)
    if result['action'] == 'updated':
        log_update(record)


───────────────────────────────────────────────────────────────────────────
TROUBLESHOOTING QUICK FIXES
───────────────────────────────────────────────────────────────────────────

Issue              Quick Fix
─────────────────────────────────────────────────────────────────────────
Connection error   Check config.py (IP, port, user, password)
Records not        Add UNIQUE(patientuuid, quarter) constraint
  updating
Slow performance   Create indexes (see SCHEMA_SETUP.sql)
                   Reduce batch size to 3000-5000 records
Module not found   from dao import postgresquarterupsert
                   (NOT: import postgresquarterupsert)
Touchtime issues   Ensure new records have datetime value
                   Check timezones match


───────────────────────────────────────────────────────────────────────────
KEY TAKEAWAYS
───────────────────────────────────────────────────────────────────────────

1. ✓ Required fields: patientuuid, quarter, touchtime
2. ✓ Required constraint: UNIQUE(patientuuid, quarter)
3. ✓ Imports: from dao import postgresdao, postgresquarterupsert
4. ✓ Main function: batch_upsert_by_quarter()
5. ✓ Return: {"inserted": N, "updated": M, "skipped": K}
6. ✓ Always close connection: conn.close()
7. ✓ Validate setup: python quickstart_demo.py


───────────────────────────────────────────────────────────────────────────
NEXT STEPS
───────────────────────────────────────────────────────────────────────────

1. Run validation:        python quickstart_demo.py
2. Review full docs:      Read QUARTER_UPSERT_README.md
3. Create your table:     Use SCHEMA_SETUP.sql template
4. Run examples:          python examples_quarter_upsert.py
5. Integrate to code:     from dao import postgresquarterupsert
6. Monitor performance:   Check execution times and log files


═════════════════════════════════════════════════════════════════════════════
For detailed documentation, see:
  - dao/postgresquarterupsert.py (API reference)
  - QUARTER_UPSERT_README.md (Complete guide)
  - INTEGRATION_GUIDE.py (Integration patterns)
  - examples_quarter_upsert.py (Working examples)
═════════════════════════════════════════════════════════════════════════════
"""

if __name__ == "__main__":
    print(QUICK_REFERENCE)

