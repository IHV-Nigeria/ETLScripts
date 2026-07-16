"""
PostgreSQL Quarter-based Upsert Module

This module provides functionality for upserting records based on patientuuid and quarter
with touchtime comparison logic:
- If record doesn't exist: insert new record
- If record exists (same patientuuid & quarter):
  - Compare touchtime: if DB touchtime < record touchtime: update whole record
  - If DB touchtime >= record touchtime: skip the record
- If quarter is different: insert as new record
"""

from . import config as postgres_config
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values
from psycopg2 import sql
import logging
from datetime import datetime


def _normalize_record_keys(record):
    """Return a copy of the record with lowercase keys."""
    return {str(key).lower(): value for key, value in record.items()}


def ensure_quarter_upsert_index(conn, table_name, auto_create=True):
    """
    Ensure a unique index exists for (patientuuid, quarter), required by ON CONFLICT.
    If duplicates exist, automatically deduplicate by keeping newest touchtime per key.

    Returns:
        True if an appropriate unique index/constraint exists (or was created).

    Raises:
        RuntimeError: If no unique index exists and cannot be created.
    """
    if conn is None:
        raise RuntimeError("PostgreSQL connection is None.")

    exists_sql = """
        SELECT EXISTS (
            SELECT 1
            FROM pg_index i
            JOIN pg_class t ON t.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relname = %s
              AND n.nspname = current_schema()
              AND i.indisunique
              AND (
                  SELECT array_agg(a.attname ORDER BY x.ord)
                  FROM unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
                  JOIN pg_attribute a
                    ON a.attrelid = t.oid
                   AND a.attnum = x.attnum
              ) = ARRAY['patientuuid', 'quarter']::name[]
        );
    """

    with conn.cursor() as cur:
        cur.execute(exists_sql, (table_name,))
        has_index = cur.fetchone()[0]

    if has_index:
        return True

    if not auto_create:
        raise RuntimeError(
            f"Table '{table_name}' is missing a unique index/constraint on (patientuuid, quarter)."
        )

    # Auto-deduplicate: Keep newest (by touchtime DESC, then ctid DESC) per (patientuuid, quarter)
    print(f"[PREFLIGHT] Checking for duplicate (patientuuid, quarter) keys in {table_name}...")
    
    dup_check_sql = sql.SQL("""
        SELECT COUNT(*) FROM (
            SELECT patientuuid, quarter, COUNT(*) AS c
            FROM {}
            GROUP BY patientuuid, quarter
            HAVING COUNT(*) > 1
        ) t;
    """).format(sql.Identifier(table_name))
    
    with conn.cursor() as cur:
        cur.execute(dup_check_sql)
        dup_count = cur.fetchone()[0]
    
    if dup_count > 0:
        print(f"[PREFLIGHT] Found {dup_count} duplicate (patientuuid, quarter) groups. Auto-deduplicating...")
        
        dedupe_sql = sql.SQL("""
            DELETE FROM {table_name} t
            USING (
                WITH ranked AS (
                    SELECT ctid,
                           ROW_NUMBER() OVER (
                               PARTITION BY patientuuid, quarter
                               ORDER BY touchtime DESC NULLS LAST, ctid DESC
                           ) AS rn
                    FROM {table_name}
                )
                SELECT ctid FROM ranked WHERE rn > 1
            ) to_delete
            WHERE t.ctid = to_delete.ctid;
        """).format(table_name=sql.Identifier(table_name))
        
        try:
            with conn.cursor() as cur:
                cur.execute(dedupe_sql)
            conn.commit()
            with conn.cursor() as cur:
                deleted = cur.rowcount
            print(f"[PREFLIGHT] Deleted {deleted} duplicate rows. Keeping newest by touchtime.")
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Failed to auto-deduplicate {table_name}: {e}")

    index_name = f"ux_{table_name}_patientuuid_quarter"
    create_index_sql = sql.SQL(
        "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({}, {});"
    ).format(
        sql.Identifier(index_name),
        sql.Identifier(table_name),
        sql.Identifier("patientuuid"),
        sql.Identifier("quarter"),
    )

    try:
        with conn.cursor() as cur:
            cur.execute(create_index_sql)
        conn.commit()
        print(f"[PREFLIGHT] Created unique index {index_name} on {table_name}(patientuuid, quarter).")
        return True
    except Exception:
        conn.rollback()
        raise RuntimeError(
            f"Failed to create unique index on {table_name}(patientuuid, quarter). "
            f"Ensure columns (patientuuid, quarter) exist and/or check permissions."
        )


def batch_upsert_by_quarter(conn, table_name, records_list, protected_keys=None):
    """
    Performs batch upsert on records using patientuuid and quarter as composite key.
    
    Logic:
    - If record doesn't exist: INSERT new record
    - If record exists with same quarter:
      - Compare touchtime: if DB touchtime < record touchtime: UPDATE
      - If DB touchtime >= record touchtime: SKIP
    - If quarter is different: INSERT as new record (creates separate entry)
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the target table
        records_list: List of dictionaries representing records to upsert
        protected_keys: Set of column names not to update (default: {'recordid', 'patientuuid', 'quarter'})
    
    Returns:
        Dictionary with counts: {"inserted": int, "updated": int, "skipped": int}
    
    Raises:
        Exception: If database operation fails
    """
    
    logging.basicConfig(
        filename='postgresql_errors.log',
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if not records_list:
        return {"inserted": 0, "updated": 0, "skipped": 0}
    
    # Normalize all records to have lowercase keys
    normalized_records = []
    for r in records_list:
        normalized = _normalize_record_keys(r)
        # Validate required keys exist
        if normalized.get('patientuuid') is not None and normalized.get('quarter') is not None:
            normalized_records.append(normalized)
    
    skipped_count = len(records_list) - len(normalized_records)
    
    if not normalized_records:
        return {"inserted": 0, "updated": 0, "skipped": skipped_count}
    
    # Set default protected keys if not provided
    if protected_keys is None:
        protected_keys = {'recordid', 'patientuuid', 'quarter'}
    
    # Extract columns and determine which ones to update
    columns = list(normalized_records[0].keys())
    update_columns = [col for col in columns if col.lower() not in protected_keys]
    
    if not update_columns:
        print(f"Warning: No columns available for update. Protected keys: {protected_keys}")
    
    # Build the UPDATE clause with EXCLUDED values
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
    
    # SQL for upsert with touchtime comparison
    # The RETURNING xmax clause allows us to determine if operation was INSERT (xmax=0) or UPDATE (xmax≠0)
    sql = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (patientuuid, quarter)
        DO UPDATE SET 
            {update_clause}
        WHERE {table_name}.touchtime IS NULL
           OR (EXCLUDED.touchtime IS NOT NULL AND EXCLUDED.touchtime > {table_name}.touchtime)
        RETURNING (xmax = 0) AS is_insert;
    """
    
    # Prepare values as list of tuples
    values = [[r[col] for col in columns] for r in normalized_records]
    
    ins_count = 0
    upd_count = 0
    
    try:
        with conn.cursor() as cur:
            # execute_values with fetch=True returns the RETURNING rows
            results = execute_values(cur, sql, values, fetch=True) or []
            
            for row in results:
                if row[0]:  # is_insert is True
                    ins_count += 1
                else:  # is_insert is False (it was an update)
                    upd_count += 1
            
            conn.commit()
            print(f"Upsert completed: {ins_count} inserted, {upd_count} updated, {len(normalized_records) - len(results) + skipped_count} skipped")
            
    except Exception as e:
        logging.error(f"Error processing batch: {e}. Sample record: {normalized_records[0] if normalized_records else 'None'}")
        conn.rollback()
        print(f"Database Error: {e}")
        raise e
    
    # Calculate final skipped count
    skipped_count += max(0, len(normalized_records) - len(results))
    
    return {"inserted": ins_count, "updated": upd_count, "skipped": skipped_count}


def batch_upsert_by_quarter_with_tracking(conn, table_name, records_list, protected_keys=None):
    """
    Performs batch upsert with detailed tracking of inserts, updates, and errors.
    
    Returns detailed information for logging:
    - inserted_details: List of {patientuuid, datim_code, uniqueID}
    - updated_details: List of {patientuuid, datim_code, uniqueID, old_touchtime, new_touchtime}
    - error_details: List of {patientuuid, datim_code, uniqueID, error_message}
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the target table
        records_list: List of dictionaries representing records to upsert
        protected_keys: Set of column names not to update (default: {'recordid', 'patientuuid', 'quarter'})
    
    Returns:
        Dictionary with counts and details:
        {
            "inserted": int,
            "updated": int,
            "skipped": int,
            "errors": int,
            "inserted_details": [...],
            "updated_details": [...],
            "error_details": [...]
        }
    """
    
    logging.basicConfig(
        filename='postgresql_errors.log',
        level=logging.ERROR,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if not records_list:
        return {
            "inserted": 0, "updated": 0, "skipped": 0, "errors": 0,
            "inserted_details": [], "updated_details": [], "error_details": []
        }
    
    # Normalize all records to have lowercase keys
    normalized_records = []
    for r in records_list:
        normalized = _normalize_record_keys(r)
        # Validate required keys exist
        if normalized.get('patientuuid') is not None and normalized.get('quarter') is not None:
            normalized_records.append(normalized)
    
    skipped_count = len(records_list) - len(normalized_records)
    
    if not normalized_records:
        return {
            "inserted": 0, "updated": 0, "skipped": skipped_count, "errors": 0,
            "inserted_details": [], "updated_details": [], "error_details": []
        }
    
    # Set default protected keys if not provided
    if protected_keys is None:
        protected_keys = {'recordid', 'patientuuid', 'quarter'}
    
    # Fetch existing records to determine insert vs update
    patientuuid_list = [r.get('patientuuid') for r in normalized_records]
    key_pairs = [(r.get('patientuuid'), r.get('quarter')) for r in normalized_records]
    
    try:
        existing_map = get_existing_touchtimes_by_quarter(conn, table_name, key_pairs)
    except Exception as e:
        logging.error(f"Error fetching existing records: {e}")
        existing_map = {}
    
    # Extract columns and determine which ones to update
    columns = list(normalized_records[0].keys())
    update_columns = [col for col in columns if col.lower() not in protected_keys]
    
    if not update_columns:
        print(f"Warning: No columns available for update. Protected keys: {protected_keys}")
    
    # Build the UPDATE clause with EXCLUDED values
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
    
    # SQL for upsert with touchtime comparison
    sql_query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (patientuuid, quarter)
        DO UPDATE SET 
            {update_clause}
        WHERE {table_name}.touchtime IS NULL
           OR (EXCLUDED.touchtime IS NOT NULL AND EXCLUDED.touchtime > {table_name}.touchtime)
        RETURNING (xmax = 0) AS is_insert;
    """
    
    # Prepare values as list of tuples
    values = [[r[col] for col in columns] for r in normalized_records]
    
    ins_count = 0
    upd_count = 0
    err_count = 0
    inserted_details = []
    updated_details = []
    error_details = []
    
    try:
        with conn.cursor() as cur:
            # execute_values with fetch=True returns the RETURNING rows
            results = execute_values(cur, sql_query, values, fetch=True) or []
            
            # Track which records were actually affected by the upsert
            # Build a set of (patientuuid, quarter) pairs that got results
            result_keys = set()
            
            # Map results back to original records
            for idx, (record, result) in enumerate(zip(normalized_records, results)):
                try:
                    is_insert = result[0]
                    datim_code = record.get('datimcode', 'N/A')
                    uniqueid = record.get('uniqueid', 'N/A')
                    patientuuid = record.get('patientuuid')
                    quarter = record.get('quarter')
                    touchtime = record.get('touchtime')
                    
                    # Track this key as processed
                    result_keys.add((patientuuid, quarter))
                    
                    if is_insert:
                        ins_count += 1
                        inserted_details.append({
                            'patientuuid': patientuuid,
                            'datim_code': datim_code,
                            'uniqueID': uniqueid,
                            'quarter': quarter,
                            'touchtime': touchtime
                        })
                    else:
                        upd_count += 1
                        old_touchtime = existing_map.get((patientuuid, quarter))
                        updated_details.append({
                            'patientuuid': patientuuid,
                            'datim_code': datim_code,
                            'uniqueID': uniqueid,
                            'quarter': quarter,
                            'old_touchtime': old_touchtime,
                            'new_touchtime': touchtime
                        })
                except Exception as e:
                    logging.error(f"Error processing record result: {e}")
            
            # Process records that didn't get results (skipped by WHERE clause)
            # These are records where the WHERE condition failed (existing touchtime >= new touchtime)
            for record in normalized_records:
                patientuuid = record.get('patientuuid')
                quarter = record.get('quarter')
                
                # If this record didn't appear in results, it was skipped
                if (patientuuid, quarter) not in result_keys:
                    # This record was genuinely skipped due to touchtime check
                    # We don't track skipped records in CSV by design
                    pass
            
            conn.commit()
            print(f"Upsert completed: {ins_count} inserted, {upd_count} updated, {len(normalized_records) - len(results) + skipped_count} skipped")
            
            
    except Exception as e:
        logging.error(f"Error processing batch: {e}. Sample record: {normalized_records[0] if normalized_records else 'None'}")
        conn.rollback()
        print(f"Database Error: {e}")
        
        # Log all records as errors
        for record in normalized_records:
            error_details.append({
                'patientuuid': record.get('patientuuid'),
                'datim_code': record.get('datimcode', 'N/A'),
                'uniqueID': record.get('uniqueid', 'N/A'),
                'error': str(e)
            })
        err_count = len(normalized_records)
        raise e
    
    # Calculate final skipped count: records that were in normalized_records but not affected by upsert
    # (because the WHERE clause prevented the update)
    final_skipped_count = len(normalized_records) - ins_count - upd_count
    
    return {
        "inserted": ins_count,
        "updated": upd_count,
        "skipped": final_skipped_count,
        "errors": err_count,
        "inserted_details": inserted_details,
        "updated_details": updated_details,
        "error_details": error_details
    }


def get_existing_quarter_records(conn, table_name, patientuuid_list):
    """
    Retrieves existing records from the database for given patientuuids.
    Useful for pre-checking before batch operations.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the target table
        patientuuid_list: List of patientuuid values to search for
    
    Returns:
        Dictionary keyed by (patientuuid, quarter) with record data
    
    Raises:
        Exception: If database query fails
    """
    
    if conn is None or not patientuuid_list:
        return {}
    
    # Remove duplicates
    unique_uuids = list(set(patientuuid_list))
    
    # Build IN clause
    placeholders = ','.join(['%s'] * len(unique_uuids))
    sql = f"SELECT patientuuid, quarter, touchtime FROM {table_name} WHERE patientuuid IN ({placeholders})"
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, unique_uuids)
            rows = cur.fetchall()
            return {(row[0], row[1]): {'touchtime': row[2]} for row in rows}
    except Exception as e:
        print(f"Failed to fetch existing records: {e}")
        raise e


def get_existing_touchtimes_by_quarter(conn, table_name, key_pairs):
    """
    Returns a dictionary of existing touchtimes keyed by (patientuuid, quarter).
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the target table
        key_pairs: List of tuples [(patientuuid, quarter), ...]
    
    Returns:
        Dictionary: {(patientuuid, quarter): touchtime, ...}
    
    Raises:
        Exception: If database query fails
    """
    
    if conn is None or not key_pairs:
        return {}
    
    # Remove duplicates
    unique_key_pairs = list({(k[0], k[1]) for k in key_pairs if k[0] is not None and k[1] is not None})
    
    if not unique_key_pairs:
        return {}
    
    sql = f"""
        SELECT k.patientuuid, k.quarter, t.touchtime
        FROM (VALUES %s) AS k(patientuuid, quarter)
        LEFT JOIN {table_name} t
          ON t.patientuuid = k.patientuuid
         AND t.quarter = k.quarter;
    """
    
    try:
        with conn.cursor() as cur:
            rows = execute_values(cur, sql, unique_key_pairs, fetch=True) or []
            return {(row[0], row[1]): row[2] for row in rows}
    except Exception as e:
        print(f"Failed to fetch existing touchtimes: {e}")
        raise e


def delete_records_by_quarter(conn, table_name, patientuuid, quarter):
    """
    Deletes a specific record identified by patientuuid and quarter.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the target table
        patientuuid: The patient UUID
        quarter: The quarter value
    
    Returns:
        Boolean: True if deletion was successful
    
    Raises:
        Exception: If database operation fails
    """
    
    if conn is None:
        print("Connection is None. Cannot delete record.")
        return False
    
    sql = f"DELETE FROM {table_name} WHERE patientuuid = %s AND quarter = %s;"
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (patientuuid, quarter))
            conn.commit()
            rows_deleted = cur.rowcount
            print(f"Successfully deleted {rows_deleted} record(s).")
            return True
    except Exception as e:
        logging.error(f"Error deleting record: {e}")
        conn.rollback()
        print(f"Database Error: {e}")
        raise e


def get_records_by_quarter_range(conn, table_name, quarter_start, quarter_end, limit=None):
    """
    Retrieves records within a quarter range.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the target table
        quarter_start: Starting quarter (format: 'Q1-2024' or similar)
        quarter_end: Ending quarter
        limit: Optional row limit
    
    Returns:
        List of dictionaries representing records
    
    Raises:
        Exception: If database query fails
    """
    
    if conn is None:
        print("Connection is None.")
        return []
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    sql = f"""
        SELECT * FROM {table_name}
        WHERE quarter >= %s AND quarter <= %s
        {limit_clause};
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (quarter_start, quarter_end))
            rows = cur.fetchall()
            return rows
    except Exception as e:
        print(f"Failed to fetch records by quarter range: {e}")
        raise e


def compare_and_upsert_record(conn, table_name, new_record, protected_keys=None):
    """
    Compares a single new record with existing database record and performs upsert if needed.
    
    This is useful when you want to check touchtime before inserting.
    
    Args:
        conn: PostgreSQL connection object
        table_name: Name of the target table
        new_record: Dictionary representing the new record
        protected_keys: Set of column names not to update
    
    Returns:
        Dictionary: {"action": "inserted"|"updated"|"skipped", "details": str}
    
    Raises:
        Exception: If database operation fails
    """
    
    if protected_keys is None:
        protected_keys = {'recordid', 'patientuuid', 'quarter'}
    
    normalized = _normalize_record_keys(new_record)
    patientuuid = normalized.get('patientuuid')
    quarter = normalized.get('quarter')
    new_touchtime = normalized.get('touchtime')
    
    if not patientuuid or not quarter:
        return {"action": "skipped", "details": "Missing patientuuid or quarter"}
    
    # Check if record exists
    existing_touchtimes = get_existing_touchtimes_by_quarter(conn, table_name, [(patientuuid, quarter)])
    existing_touchtime = existing_touchtimes.get((patientuuid, quarter))
    
    if existing_touchtime is None:
        # Record doesn't exist, insert it
        result = batch_upsert_by_quarter(conn, table_name, [normalized], protected_keys)
        return {"action": "inserted", "details": f"New record inserted for {patientuuid}/{quarter}"}
    
    # Record exists, check touchtime
    if new_touchtime is None:
        return {"action": "skipped", "details": f"New record missing touchtime, skipping update"}
    
    if new_touchtime > existing_touchtime:
        # New record is newer, update
        result = batch_upsert_by_quarter(conn, table_name, [normalized], protected_keys)
        return {"action": "updated", "details": f"Record updated for {patientuuid}/{quarter} (new touchtime: {new_touchtime})"}
    else:
        # Existing record is newer or equal, skip
        return {"action": "skipped", "details": f"Existing record is newer or equal (existing: {existing_touchtime}, new: {new_touchtime})"}

