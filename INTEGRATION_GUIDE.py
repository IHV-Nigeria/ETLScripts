"""
Integration Guide: Quarter Upsert with Your Existing ETL Scripts

This guide shows how to integrate the quarter-based upsert functionality
into your existing ETL and export scripts.
"""

# ============================================================================
# EXAMPLE 1: Integrate with EACDataExportMultiprocess.py
# ============================================================================

"""
Integration approach for EACDataExportMultiprocess.py:

Original flow:
    Extract from MongoDB → Process → Export to CSV

New flow:
    Extract from MongoDB → Process → Upsert to PostgreSQL (+CSV Export)
"""

def integrate_with_eac_export():
    """
    How to modify your EACDataExportMultiprocess.py to include PostgreSQL upsert.
    
    Steps:
    1. Import the new module at the top of the script
    2. In the consumer function, after processing, upsert to PostgreSQL
    3. Track statistics (inserted, updated, skipped)
    4. Log results
    """
    
    integration_example = """
# At the top of EACDataExportMultiprocess.py
# ═══════════════════════════════════════════════════════════════════════
from dao import postgresdao
from dao import postgresquarterupsert
import logging
from datetime import datetime

# In the consumer function (after your current processing)
# ═══════════════════════════════════════════════════════════════════════

def consumer(work_queue, consumer_id, cutoff_datetime, filename):
    '''Modified consumer function with PostgreSQL upsert.'''
    
    processed_records = []
    batch_size = 1000
    
    # Connect to PostgreSQL (do this once per consumer)
    pg_conn = postgresdao.connect_to_postgresqldb()
    if not pg_conn:
        logging.error(f"Consumer {consumer_id}: Could not connect to PostgreSQL")
        return
    
    try:
        while True:
            try:
                mongo_doc = work_queue.get(timeout=30)
                
                if mongo_doc is None:  # Sentinel value
                    # Process remaining records
                    if processed_records:
                        _upsert_batch(pg_conn, processed_records)
                    work_queue.task_done()
                    break
                
                # Your existing processing logic
                processed_record = {
                    'patientuuid': mongo_doc['uuid'],
                    'quarter': _calculate_quarter(mongo_doc['visitDate']),
                    'touchtime': datetime.now(),
                    'patient_name': mongo_doc['name'],
                    'status': mongo_doc['status'],
                    'visits_count': len(mongo_doc.get('visits', [])),
                    'adherence_score': mongo_doc.get('adherence', 0),
                    # ... other fields
                }
                
                processed_records.append(processed_record)
                
                # Batch upsert every N records
                if len(processed_records) >= batch_size:
                    _upsert_batch(pg_conn, processed_records)
                    processed_records = []
                
                work_queue.task_done()
                
            except queue.Empty:
                if processed_records:
                    _upsert_batch(pg_conn, processed_records)
                    processed_records = []
                continue
    
    finally:
        if pg_conn:
            pg_conn.close()
        logging.info(f"Consumer {consumer_id} finished")

def _upsert_batch(pg_conn, records):
    '''Helper to upsert a batch of records.'''
    try:
        result = postgresquarterupsert.batch_upsert_by_quarter(
            pg_conn,
            'eac_quarterly_data',  # Your table name
            records,
            protected_keys={'recordid', 'patientuuid', 'quarter'}
        )
        logging.info(f"Upsert result: {result['inserted']} inserted, "
                    f"{result['updated']} updated, {result['skipped']} skipped")
    except Exception as e:
        logging.error(f"Upsert failed: {e}")
        raise

def _calculate_quarter(date_obj):
    '''Convert date to quarter string.'''
    month = date_obj.month
    year = date_obj.year
    quarter = (month - 1) // 3 + 1
    return f"Q{quarter}-{year}"
    """
    
    print(integration_example)


# ============================================================================
# EXAMPLE 2: Standalone ETL Script Template
# ============================================================================

def create_standalone_etl_template():
    """
    Template for a standalone script that pulls from MongoDB and
    upserts to PostgreSQL with quarter grouping.
    """
    
    template = '''
"""
Standalone ETL Script: MongoDB to PostgreSQL with Quarter Upsert
This script can be scheduled to run periodically (daily, weekly, etc.)
"""

import logging
from datetime import datetime, timedelta
from io import StringIO
import dao.mongodbdao as mongo_dao
from dao import postgresdao
from dao import postgresquarterupsert

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_quarter_upsert.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def extract_from_mongodb(db, collection_name):
    """Extract patient records from MongoDB."""
    logger.info(f"Extracting data from MongoDB collection: {collection_name}")
    
    records = []
    try:
        collection = db[collection_name]
        cursor = collection.find({})
        
        for doc in cursor:
            records.append(doc)
        
        logger.info(f"Extracted {len(records)} records from MongoDB")
        return records
    
    except Exception as e:
        logger.error(f"Error extracting from MongoDB: {e}")
        raise


def transform_for_postgresql(mongo_records, quarter):
    """Transform MongoDB records for PostgreSQL upsert."""
    logger.info(f"Transforming {len(mongo_records)} records for {quarter}")
    
    transformed = []
    
    for doc in mongo_records:
        try:
            transformed_record = {
                'patientuuid': doc.get('_id'),  # or doc.get('uuid')
                'quarter': quarter,
                'touchtime': datetime.now(),
                'patient_name': doc.get('name'),
                'status': doc.get('status'),
                'facility': doc.get('facilityCode'),
                'enrollment_date': doc.get('enrollmentDate'),
                'data_field_1': doc.get('field1'),
                'data_field_2': doc.get('field2'),
                # ... map more fields as needed
            }
            transformed.append(transformed_record)
        
        except Exception as e:
            logger.warning(f"Skipping record due to transformation error: {e}")
            continue
    
    logger.info(f"Successfully transformed {len(transformed)} records")
    return transformed


def load_to_postgresql(pg_conn, table_name, records):
    """Load transformed records to PostgreSQL with upsert logic."""
    logger.info(f"Loading {len(records)} records to {table_name}")
    
    try:
        result = postgresquarterupsert.batch_upsert_by_quarter(
            pg_conn,
            table_name,
            records,
            protected_keys={'recordid', 'patientuuid', 'quarter'}
        )
        
        logger.info(f"Load completed:")
        logger.info(f"  Inserted: {result['inserted']}")
        logger.info(f"  Updated: {result['updated']}")
        logger.info(f"  Skipped: {result['skipped']}")
        
        return result
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise


def main():
    """Main ETL execution."""
    logger.info("Starting ETL: MongoDB → PostgreSQL Quarter Upsert")
    
    try:
        # Step 1: Connect to MongoDB
        mongo_db = mongo_dao.get_db_connection('cdr')
        if not mongo_db:
            logger.error("Failed to connect to MongoDB")
            return False
        
        # Step 2: Connect to PostgreSQL
        pg_conn = postgresdao.connect_to_postgresqldb()
        if not pg_conn:
            logger.error("Failed to connect to PostgreSQL")
            return False
        
        # Step 3: Extract from MongoDB
        mongo_records = extract_from_mongodb(mongo_db, 'art_containers')
        
        if not mongo_records:
            logger.warning("No records extracted from MongoDB")
            return False
        
        # Step 4: Calculate current quarter
        now = datetime.now()
        quarter = f"Q{(now.month - 1) // 3 + 1}-{now.year}"
        
        # Step 5: Transform records
        transformed_records = transform_for_postgresql(mongo_records, quarter)
        
        if not transformed_records:
            logger.warning("No records after transformation")
            return False
        
        # Step 6: Load to PostgreSQL with upsert
        result = load_to_postgresql(
            pg_conn,
            'my_quarterly_table',
            transformed_records
        )
        
        # Step 7: Summary
        total = result['inserted'] + result['updated'] + result['skipped']
        logger.info(f"✓ ETL Complete")
        logger.info(f"  Total processed: {total}")
        logger.info(f"  Success rate: {((result['inserted'] + result['updated']) / total * 100):.1f}%")
        
        return True
    
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        return False
    
    finally:
        if pg_conn:
            pg_conn.close()
        logger.info("Connections closed")


if __name__ == "__main__":
    main()
    '''
    
    print(template)


# ============================================================================
# EXAMPLE 3: Incremental/Delta ETL
# ============================================================================

def create_delta_etl_template():
    """
    Template for incremental ETL that only processes records modified
    since the last successful run.
    """
    
    template = '''
"""
Delta ETL: Process only modified/new records since last execution
"""

import json
from datetime import datetime, timedelta
import logging

STATE_FILE = 'etl_state.json'


def load_last_execution_state():
    """Load last execution time from state file."""
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            return datetime.fromisoformat(state['last_run'])
    except (FileNotFoundError, KeyError):
        # First run, get records from last 30 days
        return datetime.now() - timedelta(days=30)


def save_execution_state():
    """Save current execution time to state file."""
    state = {
        'last_run': datetime.now().isoformat(),
        'status': 'success'
    }
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)


def extract_modified_records(mongo_db, since_datetime):
    """Extract only records modified since last execution."""
    logger = logging.getLogger(__name__)
    
    logger.info(f"Extracting records modified since {since_datetime}")
    
    collection = mongo_db['art_containers']
    query = {
        '$or': [
            {'createdOn': {'$gte': since_datetime}},
            {'updatedOn': {'$gte': since_datetime}}
        ]
    }
    
    records = list(collection.find(query))
    logger.info(f"Found {len(records)} modified records")
    
    return records


def main():
    """Delta ETL execution."""
    logger = logging.getLogger(__name__)
    
    try:
        # Get last execution time
        last_run = load_last_execution_state()
        logger.info(f"Last run: {last_run}")
        
        # Extract only modified records
        mongo_db = mongo_dao.get_db_connection('cdr')
        records = extract_modified_records(mongo_db, last_run)
        
        if not records:
            logger.info("No new/modified records found")
            return True
        
        # Transform and load (same as standalone template)
        pg_conn = postgresdao.connect_to_postgresqldb()
        # ... rest of processing ...
        
        # Save state for next run
        save_execution_state()
        logger.info("Delta ETL completed successfully")
        
        return True
    
    except Exception as e:
        logger.error(f"Delta ETL failed: {e}")
        return False
    '''
    
    print(template)


# ============================================================================
# EXAMPLE 4: Integration with Existing Extractors
# ============================================================================

example_4 = """
INTEGRATION POINTS IN YOUR EXISTING CODEBASE:

From your directory structure, these are likely integration points:

1. Extractors/ Scripts
   ├── ARTLineListExtractor.py
   ├── CDRLineListExtractor.py
   ├── CTDLineListExtractor.py
   ├── regimenExtractor.py
   ├── IRCE_FEMIExtractor.py
   └── TBOutcomeStudy.py
   
   Integration pattern for each extractor:
   ─────────────────────────────────────────────────────────────
   
   # Before final export/save
   result = postgresquarterupsert.batch_upsert_by_quarter(
       pg_conn,
       'art_line_list_quarterly',  # or appropriate table
       extracted_records,
       protected_keys={'recordid', 'patientuuid', 'quarter'}
   )
   
   # Log results before CSV export
   print(f"PostgreSQL upsert: {result['inserted']} inserted, "
         f"{result['updated']} updated, {result['skipped']} skipped")


2. ETL/ Export Scripts
   ├── EACDataExportMultiprocess.py (See EXAMPLE 1 above)
   ├── IITEpisodeExport.py
   ├── IRCE_FEMIExport.py
   ├── regimenExport.py
   ├── TBOutcomeStudy.py
   └── transitDataExport.py
   
   Integration pattern:
   ─────────────────────────────────────────────────────────────
   
   In consumer/processing function, after extracting from MongoDB:
   
   # Calculate quarter from form dates
   quarter = f"Q{(date.month - 1) // 3 + 1}-{date.year}"
   
   # Add to record
   record['quarter'] = quarter
   record['touchtime'] = datetime.now()
   
   # Batch upsert to PostgreSQL
   postgresquarterupsert.batch_upsert_by_quarter(
       pg_conn,
       'appropriate_table_name',
       records_batch
   )


3. Utils/ Helper Functions
   └── commonutils.py - Could add utility functions here:
   
   def add_quarterly_fields(record):
       '''Add quarter and touchtime to any record.'''
       from datetime import datetime
       
       # Extract date field from record
       date_field = record.get('visitDate') or record.get('dateOfVisit')
       
       if date_field:
           quarter = f"Q{(date_field.month - 1) // 3 + 1}-{date_field.year}"
           record['quarter'] = quarter
       
       record['touchtime'] = datetime.now()
       return record


SCHEDULER INTEGRATION (for automated runs):
─────────────────────────────────────────────────────────────

If you want automated daily/weekly upserts, use:

1. Windows Task Scheduler
   ─────────────────────────────────────────────────────────────
   
   Create batch file: C:\\etl_quarter_upsert.bat
   ────────────────────────────────────────────
   @echo off
   cd C:\\Users\\innoc\\PycharmProjects\\ETLScripts-main
   python standalone_etl_script.py >> C:\\etl_logs\\%date%_%time%.log 2>&1
   
   Then create scheduled task:
   - Program: C:\\etl_quarter_upsert.bat
   - Schedule: Daily at 2 AM
   - Run with highest privileges


2. Python Schedule Library
   ─────────────────────────────────────────────────────────────
   
   import schedule
   import time
   from my_etl_script import main
   
   schedule.every().day.at("02:00").do(main)
   
   while True:
       schedule.run_pending()
       time.sleep(60)


3. Cron (Linux/Mac)
   ─────────────────────────────────────────────────────────────
   
   0 2 * * * /usr/bin/python3 /path/to/standalone_etl_script.py
"""

print(example_4)


# ============================================================================
# EXAMPLE 5: Monitoring and Notifications
# ============================================================================

example_5 = """
MONITORING INTEGRATION:

Track your upsert operations with these patterns:

Pattern 1: Log Tracking
─────────────────────────────────────────────────────────────

import logging

# Configure structured logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_upsert.log'),
        logging.StreamHandler()
    ]
)

result = postgresquarterupsert.batch_upsert_by_quarter(...)

logging.info(f"Upsert Operation:")
logging.info(f"  Table: my_table")
logging.info(f"  Records processed: {sum(result.values())}")
logging.info(f"  Inserted: {result['inserted']}")
logging.info(f"  Updated: {result['updated']}")
logging.info(f"  Skipped: {result['skipped']}")
logging.info(f"  Success rate: {((result['inserted']+result['updated'])/sum(result.values())*100):.1f}%")


Pattern 2: Email Notifications
─────────────────────────────────────────────────────────────

import smtplib
from email.mime.text import MIMEText

def send_etl_status_email(result, success):
    msg = MIMEText(f'''
    ETL Quarter Upsert Result:
    ─────────────────────────
    Inserted: {result['inserted']}
    Updated: {result['updated']}
    Skipped: {result['skipped']}
    Status: {'SUCCESS' if success else 'FAILED'}
    ''')
    
    msg['Subject'] = f'ETL Status: {'OK' if success else 'ERROR'}'
    msg['From'] = 'etl@example.com'
    msg['To'] = 'admin@example.com'
    
    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    server.login('email@gmail.com', 'password')
    server.send_message(msg)
    server.quit()


Pattern 3: Database Statistics Table
─────────────────────────────────────────────────────────────

# Create logs table
CREATE TABLE etl_upsert_logs (
    log_id SERIAL PRIMARY KEY,
    execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    table_name VARCHAR(100),
    inserted INT,
    updated INT,
    skipped INT,
    total_records INT,
    success BOOLEAN,
    error_message TEXT,
    duration_seconds INT
);

# Log each operation
from datetime import datetime

start_time = datetime.now()

try:
    result = postgresquarterupsert.batch_upsert_by_quarter(...)
    
    with pg_conn.cursor() as cur:
        cur.execute('''
            INSERT INTO etl_upsert_logs 
            (table_name, inserted, updated, skipped, total_records, success, duration_seconds)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (
            'my_table',
            result['inserted'],
            result['updated'],
            result['skipped'],
            sum(result.values()),
            True,
            (datetime.now() - start_time).total_seconds()
        ))
        pg_conn.commit()
        
except Exception as e:
    with pg_conn.cursor() as cur:
        cur.execute('''
            INSERT INTO etl_upsert_logs 
            (table_name, total_records, success, error_message, duration_seconds)
            VALUES (%s, %s, %s, %s, %s)
        ''', (
            'my_table',
            0,
            False,
            str(e),
            (datetime.now() - start_time).total_seconds()
        ))
        pg_conn.commit()
"""

print(example_5)


if __name__ == "__main__":
    print("\n" + "="*70)
    print("INTEGRATION GUIDE: Quarter Upsert with Existing ETL Scripts")
    print("="*70 + "\n")
    
    print("\n[EXAMPLE 1] Integrate with EACDataExportMultiprocess.py")
    print("─" * 70)
    integrate_with_eac_export()
    
    print("\n[EXAMPLE 2] Standalone ETL Script Template")
    print("─" * 70)
    create_standalone_etl_template()
    
    print("\n[EXAMPLE 3] Delta ETL Template (Incremental Processing)")
    print("─" * 70)
    create_delta_etl_template()
    
    print("\n[EXAMPLE 4] Integration Points in Your Codebase")
    print("─" * 70)
    print(example_4)
    
    print("\n[EXAMPLE 5] Monitoring and Notifications")
    print("─" * 70)
    print(example_5)
    
    print("\n" + "="*70)
    print("For more information, see:")
    print("  - dao/postgresquarterupsert.py (main module)")
    print("  - examples_quarter_upsert.py (usage examples)")
    print("  - QUARTER_UPSERT_README.md (complete documentation)")
    print("="*70 + "\n")

