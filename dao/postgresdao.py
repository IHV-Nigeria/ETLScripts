# import postgreconfig to ensure the file is created and available for use
from . import config as postgres_config
import psycopg2
from psycopg2 import Error
from psycopg2.extras import execute_values
import logging

def connect_to_postgresqldb():
    connection = None
    try:
        # Connect to an existing database
        connection = psycopg2.connect(
            user=postgres_config.DB_USER,
            password=postgres_config.DB_PASS,
            host=postgres_config.DB_HOST,
            port=postgres_config.DB_PORT,
            database=postgres_config.DB_NAME
        )

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        
        # Executing a SQL query to verify connection
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print(f"You are connected to - {record}\n")

        return connection

    except (Exception, Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None


def batch_upsert_art_line_list(conn, records_list):
    logging.basicConfig(filename='postgresql_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
    """
    Performs batch upsert and returns counts of inserts and updates.
    """
    if not records_list:
        return {"inserted": 0, "updated": 0, "skipped": 0}

    valid_records = [
        r for r in records_list
        if r.get('patientuuid') is not None and r.get('datimcode') is not None
    ]
    skipped_count = len(records_list) - len(valid_records)

    if not valid_records:
        return {"inserted": 0, "updated": 0, "skipped": skipped_count}

    columns = list(valid_records[0].keys())
    update_columns = [col for col in columns if col not in ['recordid', 'patientuuid', 'datimcode']]
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])

    # The RETURNING xmax clause allows us to see what the DB did
    sql = f"""
        INSERT INTO art_line_list ({", ".join(columns)})
        VALUES %s
        ON CONFLICT (patientuuid, datimcode)
        DO UPDATE SET 
            {update_clause}
        WHERE art_line_list.touchtime IS NULL
           OR (EXCLUDED.touchtime IS NOT NULL AND EXCLUDED.touchtime > art_line_list.touchtime)
        RETURNING (xmax = 0) AS is_insert;
    """

    values = [[r[col] for col in columns] for r in valid_records]
    
    ins_count = 0
    upd_count = 0
    
    try:
        with conn.cursor() as cur:
            # execute_values with fetch=True returns the RETURNING rows.
            results = execute_values(cur, sql, values, fetch=True) or []
            
            for row in results:
                if row[0]: # is_insert is True
                    ins_count += 1
                else: # is_insert is False (it was an update)
                    upd_count += 1
                    
            conn.commit()
    except Exception as e:
        logging.error(f"Error processing batch: {e}. Batch details: {values}")
        conn.rollback()
        print(f"Database Error: {e}")
        raise e

    skipped_count += max(0, len(valid_records) - len(results))
    return {"inserted": ins_count, "updated": upd_count, "skipped": skipped_count}


def get_art_line_list_existing_touchtimes(conn, key_pairs):
    """
    Returns a dictionary keyed by (patientuuid, datimcode) with existing touchtime.
    """
    if conn is None or not key_pairs:
        return {}

    unique_key_pairs = list({(k[0], k[1]) for k in key_pairs if k[0] is not None and k[1] is not None})
    if not unique_key_pairs:
        return {}

    sql = """
        SELECT k.patientuuid, k.datimcode, a.touchtime
        FROM (VALUES %s) AS k(patientuuid, datimcode)
        LEFT JOIN art_line_list a
          ON a.patientuuid = k.patientuuid
         AND a.datimcode = k.datimcode;
    """

    try:
        with conn.cursor() as cur:
            rows = execute_values(cur, sql, unique_key_pairs, fetch=True) or []
            return {(row[0], row[1]): row[2] for row in rows}
    except Exception as e:
        print(f"Failed to fetch existing ART touchtimes: {e}")
        raise e

def save_to_postgres(conn, table_name, batch_data):
    if conn is None:
        print("Failed to connect to PostgreSQL. Data not saved.")
        return
    
    """
    Inserts a list of dictionaries into a PostgreSQL table in a single batch.
    
    :param db_config: Dict containing connection parameters (host, dbname, user, password)
    :param table_name: String name of the target table
    :param records: List of dictionaries where keys match table columns
    """
    if not batch_data:
        print("No records to insert.")
        return

    # Extract column names from the first dictionary
    columns = batch_data[0].keys()
    query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s"
    
    # Transform list of dicts into list of tuples for psycopg2
    values = [tuple(record.values()) for record in batch_data]

    try:
        
        cur = conn.cursor()
        
        # execute_values is significantly faster than executemany
        execute_values(cur, query, values)
        
        conn.commit()
        #print(f"Successfully inserted {len(batch_data)} records into {table_name}.")
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        
        if conn:
            conn.rollback()