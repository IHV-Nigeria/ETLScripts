"""
Example Script: PostgreSQL Quarter-Based Upsert
This script demonstrates how to use the postgresquarterupsert module
for inserting/updating records with touchtime comparison.

Usage scenarios:
1. Batch upsert from MongoDB export
2. Single record comparison before upsert
3. Pre-check existing records before processing
"""

import sys
import logging
from datetime import datetime
from typing import List, Dict

# Import the DAO modules
from dao import postgresdao
from dao import postgresquarterupsert


def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('quarter_upsert.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


def example_batch_upsert(conn, table_name: str, records: List[Dict]):
    """
    Example 1: Batch upsert multiple records at once.
    
    Args:
        conn: PostgreSQL connection
        table_name: Target table name (e.g., 'patient_quarterly_data')
        records: List of record dictionaries with patientuuid, quarter, touchtime, etc.
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting batch upsert of {len(records)} records into {table_name}")
    
    try:
        result = postgresquarterupsert.batch_upsert_by_quarter(
            conn,
            table_name,
            records,
            protected_keys={'recordid', 'patientuuid', 'quarter'}  # Don't update these columns
        )
        
        logger.info(f"Batch upsert completed:")
        logger.info(f"  Inserted: {result['inserted']}")
        logger.info(f"  Updated: {result['updated']}")
        logger.info(f"  Skipped: {result['skipped']}")
        
        return result
        
    except Exception as e:
        logger.error(f"Batch upsert failed: {e}")
        raise


def example_single_record_upsert(conn, table_name: str, record: Dict):
    """
    Example 2: Upsert a single record with comparison logging.
    
    Args:
        conn: PostgreSQL connection
        table_name: Target table name
        record: Single record dictionary
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Processing single record for patient {record.get('patientuuid')}, quarter {record.get('quarter')}")
    
    try:
        result = postgresquarterupsert.compare_and_upsert_record(
            conn,
            table_name,
            record
        )
        
        logger.info(f"Action: {result['action']} - {result['details']}")
        return result
        
    except Exception as e:
        logger.error(f"Single record upsert failed: {e}")
        raise


def example_pre_check_existing_records(conn, table_name: str, patientuuid_list: List[str]):
    """
    Example 3: Pre-check if records exist in database before processing.
    
    Args:
        conn: PostgreSQL connection
        table_name: Target table name
        patientuuid_list: List of patientuuids to check
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Pre-checking {len(patientuuid_list)} patient records")
    
    try:
        existing_records = postgresquarterupsert.get_existing_quarter_records(
            conn,
            table_name,
            patientuuid_list
        )
        
        logger.info(f"Found {len(existing_records)} existing records")
        for (uuid, quarter), data in existing_records.items():
            logger.debug(f"  Patient {uuid}, Quarter {quarter}: touchtime = {data['touchtime']}")
        
        return existing_records
        
    except Exception as e:
        logger.error(f"Pre-check failed: {e}")
        raise


def example_process_mongo_export(conn, table_name: str, mongo_documents: List[Dict]):
    """
    Example 4: Process exported documents from MongoDB and upsert to PostgreSQL.
    
    This function assumes the MongoDB documents have the required fields:
    - patientuuid (unique patient identifier)
    - quarter (time period, e.g., 'Q1-2024')
    - touchtime (timestamp of last modification)
    - other_field_1, other_field_2, etc.
    
    Args:
        conn: PostgreSQL connection
        table_name: Target table name
        mongo_documents: List of documents from MongoDB
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Processing {len(mongo_documents)} documents from MongoDB export")
    
    # Validate and prepare records
    valid_records = []
    invalid_records = []
    
    for doc in mongo_documents:
        # Check required fields
        if 'patientuuid' in doc and 'quarter' in doc:
            # Ensure touchtime is set (if not provided, use current time)
            if 'touchtime' not in doc:
                doc['touchtime'] = datetime.now()
            valid_records.append(doc)
        else:
            invalid_records.append(doc)
    
    logger.info(f"Valid records: {len(valid_records)}, Invalid records: {len(invalid_records)}")
    
    if invalid_records:
        logger.warning(f"Skipping {len(invalid_records)} records due to missing required fields")
        for invalid in invalid_records[:5]:  # Log first 5
            logger.warning(f"  Missing fields in: {invalid}")
    
    # Perform batch upsert
    if valid_records:
        try:
            result = postgresquarterupsert.batch_upsert_by_quarter(
                conn,
                table_name,
                valid_records
            )
            logger.info(f"MongoDB export upsert completed: {result}")
            return result
        except Exception as e:
            logger.error(f"MongoDB export upsert failed: {e}")
            raise
    
    return {"inserted": 0, "updated": 0, "skipped": len(mongo_documents)}


def example_get_records_by_quarter(conn, table_name: str, quarter_start: str, quarter_end: str):
    """
    Example 5: Retrieve records within a quarter range.
    
    Args:
        conn: PostgreSQL connection
        table_name: Target table name
        quarter_start: Starting quarter (e.g., 'Q1-2024')
        quarter_end: Ending quarter (e.g., 'Q4-2024')
    """
    logger = logging.getLogger(__name__)
    
    logger.info(f"Retrieving records from {quarter_start} to {quarter_end}")
    
    try:
        records = postgresquarterupsert.get_records_by_quarter_range(
            conn,
            table_name,
            quarter_start,
            quarter_end,
            limit=1000
        )
        
        logger.info(f"Retrieved {len(records)} records")
        return records
        
    except Exception as e:
        logger.error(f"Quarter range retrieval failed: {e}")
        raise


def example_delete_specific_record(conn, table_name: str, patientuuid: str, quarter: str):
    """
    Example 6: Delete a specific record by patientuuid and quarter.
    
    Args:
        conn: PostgreSQL connection
        table_name: Target table name
        patientuuid: Patient UUID
        quarter: Quarter value
    """
    logger = logging.getLogger(__name__)
    
    logger.warning(f"Deleting record: {patientuuid}, {quarter}")
    
    try:
        success = postgresquarterupsert.delete_records_by_quarter(
            conn,
            table_name,
            patientuuid,
            quarter
        )
        
        if success:
            logger.info("Record deleted successfully")
        
        return success
        
    except Exception as e:
        logger.error(f"Record deletion failed: {e}")
        raise


def main():
    """Main function to demonstrate usage."""
    logger = setup_logging()
    
    try:
        # Connect to PostgreSQL
        logger.info("Connecting to PostgreSQL...")
        conn = postgresdao.connect_to_postgresqldb()
        
        if not conn:
            logger.error("Failed to connect to PostgreSQL")
            return
        
        # Define table name
        table_name = "patient_quarterly_data"  # Change to your actual table name
        
        # Example 1: Batch upsert
        sample_records = [
            {
                'patientuuid': 'uuid-001',
                'quarter': 'Q1-2024',
                'touchtime': datetime(2024, 1, 15, 10, 30),
                'data_field_1': 'value1',
                'data_field_2': 'value2'
            },
            {
                'patientuuid': 'uuid-002',
                'quarter': 'Q1-2024',
                'touchtime': datetime(2024, 1, 16, 14, 45),
                'data_field_1': 'value3',
                'data_field_2': 'value4'
            }
        ]
        
        # Uncomment to run examples:
        # example_batch_upsert(conn, table_name, sample_records)
        # example_single_record_upsert(conn, table_name, sample_records[0])
        # example_pre_check_existing_records(conn, table_name, ['uuid-001', 'uuid-002'])
        # example_process_mongo_export(conn, table_name, sample_records)
        # example_get_records_by_quarter(conn, table_name, 'Q1-2024', 'Q4-2024')
        # example_delete_specific_record(conn, table_name, 'uuid-001', 'Q1-2024')
        
        logger.info("Examples configured. Uncomment in main() to run.")
        
        # Close connection
        if conn:
            conn.close()
            logger.info("Connection closed")
        
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        raise


if __name__ == "__main__":
    main()

