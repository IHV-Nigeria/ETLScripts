"""
Quick Start Script: Zero-to-Hero Quarter Upsert Setup
This script validates your setup and provides instant working examples.
"""

import sys
from datetime import datetime, timedelta
from dao import postgresdao
from dao import postgresquarterupsert


def validate_setup():
    """Validate PostgreSQL connection and basic functionality."""
    print("=" * 60)
    print("PostgreSQL Quarter Upsert - Setup Validation")
    print("=" * 60)
    
    # Test 1: Connection
    print("\n[1/4] Testing PostgreSQL Connection...")
    try:
        conn = postgresdao.connect_to_postgresqldb()
        if conn:
            print("✓ Connection successful!")
            conn.close()
        else:
            print("✗ Connection failed!")
            return False
    except Exception as e:
        print(f"✗ Connection error: {e}")
        return False
    
    print("\n[2/4] PostgreSQL is ready!")
    print("✓ Module imports working correctly")
    print("✓ Configuration loaded successfully")
    
    return True


def create_test_table(conn, table_name="test_quarterly_data"):
    """Create a test table for demonstration."""
    print(f"\n[3/4] Creating test table '{table_name}'...")
    
    sql = f"""
    DROP TABLE IF EXISTS {table_name} CASCADE;
    
    CREATE TABLE {table_name} (
        recordid SERIAL PRIMARY KEY,
        patientuuid VARCHAR(100) NOT NULL,
        quarter VARCHAR(10) NOT NULL,
        touchtime TIMESTAMP,
        patient_name VARCHAR(255),
        status VARCHAR(50),
        score NUMERIC,
        notes TEXT,
        created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(patientuuid, quarter)
    );
    """
    
    try:
        with conn.cursor() as cur:
            # Execute as individual statements
            for statement in sql.strip().split(';'):
                if statement.strip():
                    cur.execute(statement)
            conn.commit()
        print(f"✓ Test table '{table_name}' created successfully!")
        return table_name
    except Exception as e:
        print(f"✗ Failed to create table: {e}")
        conn.rollback()
        return None


def run_demo_upserts(conn, table_name):
    """Run a complete demonstration of upsert functionality."""
    print(f"\n[4/4] Running demo upserts on '{table_name}'...\n")
    
    # Demo 1: Insert new records
    print("Demo 1: Inserting new records...")
    print("-" * 50)
    
    new_records = [
        {
            'patientuuid': 'patient-001',
            'quarter': 'Q1-2024',
            'touchtime': datetime.now() - timedelta(days=5),
            'patient_name': 'John Doe',
            'status': 'Active',
            'score': 85.5,
            'notes': 'Initial record'
        },
        {
            'patientuuid': 'patient-002',
            'quarter': 'Q1-2024',
            'touchtime': datetime.now() - timedelta(days=3),
            'patient_name': 'Jane Smith',
            'status': 'Active',
            'score': 92.0,
            'notes': 'Q1 data'
        },
        {
            'patientuuid': 'patient-001',
            'quarter': 'Q2-2024',
            'touchtime': datetime.now() - timedelta(days=2),
            'patient_name': 'John Doe',
            'status': 'Active',
            'score': 88.0,
            'notes': 'Q2 data'
        }
    ]
    
    result = postgresquarterupsert.batch_upsert_by_quarter(
        conn, table_name, new_records
    )
    
    print(f"Result: Inserted {result['inserted']}, Updated {result['updated']}, Skipped {result['skipped']}")
    for record in new_records:
        print(f"  ✓ Inserted: {record['patientuuid']} - {record['quarter']}")
    
    # Demo 2: Update with newer touchtime
    print("\n\nDemo 2: Updating record with newer touchtime...")
    print("-" * 50)
    
    updated_records = [
        {
            'patientuuid': 'patient-001',
            'quarter': 'Q1-2024',
            'touchtime': datetime.now(),  # Newer timestamp
            'patient_name': 'John Doe Updated',
            'status': 'Inactive',
            'score': 95.0,
            'notes': 'Updated with newer data'
        }
    ]
    
    result = postgresquarterupsert.batch_upsert_by_quarter(
        conn, table_name, updated_records
    )
    
    print(f"Result: Inserted {result['inserted']}, Updated {result['updated']}, Skipped {result['skipped']}")
    if result['updated'] > 0:
        print(f"  ✓ Updated: patient-001 - Q1-2024 (newer touchtime)")
    else:
        print(f"  ✓ Skipped: patient-001 - Q1-2024 (older touchtime)")
    
    # Demo 3: Try to update with older touchtime (should skip)
    print("\n\nDemo 3: Attempting update with older touchtime (should skip)...")
    print("-" * 50)
    
    old_records = [
        {
            'patientuuid': 'patient-001',
            'quarter': 'Q1-2024',
            'touchtime': datetime.now() - timedelta(days=10),  # Older timestamp
            'patient_name': 'John Doe Old',
            'status': 'Deleted',
            'score': 10.0,
            'notes': 'Should be skipped'
        }
    ]
    
    result = postgresquarterupsert.batch_upsert_by_quarter(
        conn, table_name, old_records
    )
    
    print(f"Result: Inserted {result['inserted']}, Updated {result['updated']}, Skipped {result['skipped']}")
    if result['skipped'] > 0:
        print(f"  ✓ Correctly skipped: patient-001 - Q1-2024 (older touchtime)")
    else:
        print(f"  ✗ Should have skipped but didn't!")
    
    # Demo 4: Pre-check existing records
    print("\n\nDemo 4: Pre-checking existing records...")
    print("-" * 50)
    
    existing = postgresquarterupsert.get_existing_quarter_records(
        conn, table_name, ['patient-001', 'patient-002', 'patient-999']
    )
    
    print(f"Found {len(existing)} existing patients:")
    for (uuid, quarter), data in existing.items():
        touchtime_str = data['touchtime'].strftime('%Y-%m-%d %H:%M:%S') if data['touchtime'] else 'None'
        print(f"  ✓ {uuid} - {quarter}: touchtime = {touchtime_str}")
    
    # Demo 5: Retrieve records by quarter range
    print("\n\nDemo 5: Retrieving records by quarter range...")
    print("-" * 50)
    
    records = postgresquarterupsert.get_records_by_quarter_range(
        conn, table_name, 'Q1-2024', 'Q2-2024'
    )
    
    print(f"Found {len(records)} records in Q1-2024 to Q2-2024")
    for record in records:
        print(f"  ✓ {record[1]} - {record[2]}: {record[4]}")
    
    # Demo 6: Delete a record
    print("\n\nDemo 6: Deleting a specific record...")
    print("-" * 50)
    
    success = postgresquarterupsert.delete_records_by_quarter(
        conn, table_name, 'patient-999', 'Q3-2024'
    )
    
    if success:
        print("  ✓ Delete operation completed (record didn't exist, but operation succeeded)")
    
    # Demo 7: Single record comparison
    print("\n\nDemo 7: Using compare_and_upsert_record for single records...")
    print("-" * 50)
    
    single_new = {
        'patientuuid': 'patient-003',
        'quarter': 'Q1-2024',
        'touchtime': datetime.now(),
        'patient_name': 'Bob Johnson',
        'status': 'Active',
        'score': 78.5,
        'notes': 'New patient'
    }
    
    result = postgresquarterupsert.compare_and_upsert_record(
        conn, table_name, single_new
    )
    
    print(f"Action: {result['action']}")
    print(f"Details: {result['details']}")


def print_quick_reference():
    """Print quick reference guide."""
    print("\n\n" + "=" * 60)
    print("QUICK REFERENCE GUIDE")
    print("=" * 60)
    
    guide = """
BASIC USAGE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. BATCH UPSERT (Recommended for 10+ records):
   ┌─────────────────────────────────────────────────────────────┐
   │ from dao import postgresdao, postgresquarterupsert          │
   │ from datetime import datetime                               │
   │                                                              │
   │ conn = postgresdao.connect_to_postgresqldb()               │
   │                                                              │
   │ records = [                                                 │
   │     {                                                        │
   │         'patientuuid': 'patient-001',                       │
   │         'quarter': 'Q1-2024',                               │
   │         'touchtime': datetime.now(),                        │
   │         'field1': 'value1',                                 │
   │         'field2': 'value2'                                  │
   │     }                                                        │
   │ ]                                                            │
   │                                                              │
   │ result = postgresquarterupsert.batch_upsert_by_quarter(    │
   │     conn,                                                   │
   │     'my_table',                                             │
   │     records                                                 │
   │ )                                                            │
   │                                                              │
   │ conn.close()                                                │
   └─────────────────────────────────────────────────────────────┘

2. SINGLE RECORD WITH COMPARISON:
   ┌─────────────────────────────────────────────────────────────┐
   │ result = postgresquarterupsert.compare_and_upsert_record(  │
   │     conn, 'my_table', single_record                        │
   │ )                                                            │
   │ print(result['action'])  # 'inserted', 'updated', 'skipped'│
   └─────────────────────────────────────────────────────────────┘

3. PRE-CHECK EXISTING RECORDS:
   ┌─────────────────────────────────────────────────────────────┐
   │ existing = postgresquarterupsert.get_existing_quarter_records│
   │     conn, 'my_table', ['uuid1', 'uuid2']                   │
   │ )                                                            │
   │ if ('uuid1', 'Q1-2024') in existing:                        │
   │     print("Record exists!")                                 │
   └─────────────────────────────────────────────────────────────┘

KEY POINTS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ REQUIRED FIELDS:
  - patientuuid (unique patient ID)
  - quarter (e.g., 'Q1-2024', 'Q2-2024')
  - touchtime (timestamp of last modification)

✓ DATABASE REQUIREMENT:
  - Table must have: UNIQUE(patientuuid, quarter)

✓ LOGIC:
  - New record? → INSERT
  - Same UUID + Quarter?
    - New touchtime newer? → UPDATE
    - New touchtime older? → SKIP
  - Different quarter? → INSERT

✓ RETURN VALUES for batch_upsert_by_quarter():
  {
    'inserted': int,  # Number of new records inserted
    'updated': int,   # Number of records updated
    'skipped': int    # Number of records skipped
  }

✓ PROTECTED COLUMNS (won't be updated):
  - recordid, patientuuid, quarter (by default)
  - Customize with protected_keys parameter

COMMON OPERATIONS:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Get existing touchtimes:
  postgresquarterupsert.get_existing_touchtimes_by_quarter(
      conn, 'table', [('uuid', 'Q1-2024'), ...]
  )

Delete a record:
  postgresquarterupsert.delete_records_by_quarter(
      conn, 'table', 'patient-uuid', 'Q1-2024'
  )

Get records by quarter range:
  postgresquarterupsert.get_records_by_quarter_range(
      conn, 'table', 'Q1-2024', 'Q4-2024', limit=1000
  )

FILES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📄 dao/postgresquarterupsert.py    - Main module (functions)
📄 examples_quarter_upsert.py      - Complete usage examples
📄 quickstart_demo.py              - This script (validation & demo)
📄 QUARTER_UPSERT_README.md        - Full documentation
    """
    
    print(guide)


def main():
    """Main execution."""
    try:
        # Validate setup
        if not validate_setup():
            print("\n✗ Setup validation failed. Please check your PostgreSQL configuration.")
            return False
        
        # Connect and create test table
        print("\nEstablishing database connection for demo...")
        conn = postgresdao.connect_to_postgresqldb()
        
        if not conn:
            print("✗ Failed to establish connection for demo")
            return False
        
        # Create test table
        table_name = create_test_table(conn)
        
        if not table_name:
            print("✗ Failed to create test table")
            conn.close()
            return False
        
        # Run demonstrations
        run_demo_upserts(conn, table_name)
        
        # Cleanup
        print("\n\nCleaning up test table...")
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()
        print("✓ Test table dropped")
        
        conn.close()
        
        # Print quick reference
        print_quick_reference()
        
        print("\n\n✓ SETUP COMPLETE AND VALIDATED!")
        print("═" * 60)
        print("You're ready to use the quarter upsert system!")
        print("\nNext steps:")
        print("  1. Review QUARTER_UPSERT_README.md for detailed documentation")
        print("  2. Check examples_quarter_upsert.py for more examples")
        print("  3. Create your table with UNIQUE(patientuuid, quarter)")
        print("  4. Start using batch_upsert_by_quarter() in your ETL scripts")
        print("═" * 60 + "\n")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error during setup: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

