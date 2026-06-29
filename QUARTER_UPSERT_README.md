# PostgreSQL Quarter-Based Upsert System

## Overview

This system provides robust PostgreSQL upsert functionality with `patientuuid` and `quarter` as composite keys, featuring intelligent `touchtime` comparison to determine whether records should be updated or skipped.

## Key Features

✅ **Composite Key Upsert**: Uses `patientuuid` and `quarter` as unique identifier combination
✅ **Touchtime Comparison**: Automatically compares record timestamps to decide insert/update/skip
✅ **Batch Operations**: Efficient bulk upsert operations with transaction support
✅ **Flexible Column Protection**: Configurable columns that should not be updated
✅ **Comprehensive Logging**: Detailed error and operation logging
✅ **Insert/Update/Skip Tracking**: Returns counts for each operation type

## Logic Flow

```
INPUT: New Record with patientuuid, quarter, touchtime
        ↓
    ✓ Check if record exists (by patientuuid + quarter)
        ↓
    ├─→ NO: INSERT new record
    │
    └─→ YES: Existing record found
              ↓
          Compare touchtime:
              ↓
          ├─→ DB touchtime IS NULL OR (Record touchtime > DB touchtime)
          │   ↓
          │   UPDATE record with new values
          │
          └─→ Record touchtime <= DB touchtime
              ↓
              SKIP (don't modify)
```

## Installation

### Prerequisites
- Python 3.7+
- PostgreSQL database
- psycopg2 Python package

### Setup

1. Ensure the module files are in your project:
   ```
   dao/
   ├── config.py
   ├── postgresdao.py
   ├── postgresquarterupsert.py  (NEW)
   └── mongodbdao.py
   ```

2. Update your PostgreSQL config in `dao/config.py` if needed

3. Ensure your target table has the composite unique constraint:
   ```sql
   CREATE TABLE patient_quarterly_data (
       recordid SERIAL PRIMARY KEY,
       patientuuid UUID NOT NULL,
       quarter VARCHAR(10) NOT NULL,
       touchtime TIMESTAMP,
       -- other columns...
       UNIQUE(patientuuid, quarter)  -- IMPORTANT!
   );
   ```

## Usage

### Quick Start - Batch Upsert

```python
from dao import postgresdao
from dao import postgresquarterupsert
from datetime import datetime

# Connect to PostgreSQL
conn = postgresdao.connect_to_postgresqldb()

# Prepare records
records = [
    {
        'patientuuid': 'abc-123-def',
        'quarter': 'Q1-2024',
        'touchtime': datetime(2024, 1, 15, 10, 30),
        'field1': 'value1',
        'field2': 'value2'
    },
    {
        'patientuuid': 'xyz-789-uvw',
        'quarter': 'Q1-2024',
        'touchtime': datetime(2024, 1, 16, 14, 45),
        'field1': 'value3',
        'field2': 'value4'
    }
]

# Perform batch upsert
result = postgresquarterupsert.batch_upsert_by_quarter(
    conn,
    'patient_quarterly_data',  # table name
    records,
    protected_keys={'recordid', 'patientuuid', 'quarter'}
)

print(f"Inserted: {result['inserted']}")
print(f"Updated: {result['updated']}")
print(f"Skipped: {result['skipped']}")

# Close connection
conn.close()
```

### Single Record Upsert with Comparison

```python
# Check and upsert a single record
record = {
    'patientuuid': 'abc-123-def',
    'quarter': 'Q1-2024',
    'touchtime': datetime(2024, 1, 15, 10, 30),
    'field1': 'new_value1'
}

result = postgresquarterupsert.compare_and_upsert_record(
    conn,
    'patient_quarterly_data',
    record
)

print(f"Action: {result['action']}")  # 'inserted', 'updated', or 'skipped'
print(f"Details: {result['details']}")
```

### Pre-Check Existing Records

```python
# Check which patient records already exist
patient_uuids = ['abc-123-def', 'xyz-789-uvw', 'new-patient-id']

existing = postgresquarterupsert.get_existing_quarter_records(
    conn,
    'patient_quarterly_data',
    patient_uuids
)

# existing = {
#     ('abc-123-def', 'Q1-2024'): {'touchtime': datetime(2024, 1, 15, 10, 30)},
#     ('xyz-789-uvw', 'Q1-2024'): {'touchtime': datetime(2024, 1, 16, 14, 45)}
# }

for (uuid, quarter), data in existing.items():
    print(f"Patient {uuid}/Q{quarter} exists with touchtime: {data['touchtime']}")
```

### Retrieve Records by Quarter Range

```python
# Get all records for a specific quarter period
records = postgresquarterupsert.get_records_by_quarter_range(
    conn,
    'patient_quarterly_data',
    'Q1-2024',
    'Q4-2024',
    limit=5000
)

print(f"Retrieved {len(records)} records")
```

### Delete Record by Quarter

```python
# Delete a specific patient's quarterly record
success = postgresquarterupsert.delete_records_by_quarter(
    conn,
    'patient_quarterly_data',
    patientuuid='abc-123-def',
    quarter='Q1-2024'
)

if success:
    print("Record deleted successfully")
```

## API Reference

### batch_upsert_by_quarter()

Performs batch upsert on multiple records.

```python
def batch_upsert_by_quarter(
    conn,                    # PostgreSQL connection object
    table_name,              # String: target table name
    records_list,            # List[Dict]: records to upsert
    protected_keys=None      # Set[str]: columns not to update (optional)
) -> Dict: # Returns {"inserted": int, "updated": int, "skipped": int}
```

**Example:**
```python
result = postgresquarterupsert.batch_upsert_by_quarter(
    conn,
    'my_table',
    records,
    protected_keys={'recordid', 'patientuuid', 'quarter'}
)
```

---

### compare_and_upsert_record()

Compares a single record with existing database record and upserts if needed.

```python
def compare_and_upsert_record(
    conn,                    # PostgreSQL connection object
    table_name,              # String: target table name
    new_record,              # Dict: record to upsert
    protected_keys=None      # Set[str]: columns not to update (optional)
) -> Dict: # Returns {"action": str, "details": str}
```

**Returns:**
- `action`: "inserted" | "updated" | "skipped"
- `details`: Human-readable explanation

**Example:**
```python
result = postgresquarterupsert.compare_and_upsert_record(conn, 'my_table', record)
if result['action'] == 'inserted':
    print("New record created")
elif result['action'] == 'updated':
    print("Existing record updated")
else:
    print("Record skipped - existing record is newer")
```

---

### get_existing_quarter_records()

Retrieves existing records for given patient UUIDs.

```python
def get_existing_quarter_records(
    conn,                    # PostgreSQL connection object
    table_name,              # String: target table name
    patientuuid_list         # List[str]: patient UUIDs to search for
) -> Dict: # Returns {(patientuuid, quarter): {'touchtime': datetime}, ...}
```

---

### get_existing_touchtimes_by_quarter()

Returns existing touchtime values for specific (patientuuid, quarter) pairs.

```python
def get_existing_touchtimes_by_quarter(
    conn,                    # PostgreSQL connection object
    table_name,              # String: target table name
    key_pairs                # List[Tuple]: [(patientuuid, quarter), ...]
) -> Dict: # Returns {(patientuuid, quarter): touchtime, ...}
```

---

### delete_records_by_quarter()

Deletes a specific record by patientuuid and quarter.

```python
def delete_records_by_quarter(
    conn,                    # PostgreSQL connection object
    table_name,              # String: target table name
    patientuuid,             # String: patient UUID
    quarter                  # String: quarter value
) -> bool: # Returns True if successful
```

---

### get_records_by_quarter_range()

Retrieves records within a quarter range.

```python
def get_records_by_quarter_range(
    conn,                    # PostgreSQL connection object
    table_name,              # String: target table name
    quarter_start,           # String: starting quarter (e.g., 'Q1-2024')
    quarter_end,             # String: ending quarter (e.g., 'Q4-2024')
    limit=None               # int: optional row limit
) -> List: # Returns list of records
```

## Database Schema Requirements

Your target table MUST have:

1. **Composite Unique Constraint** on `(patientuuid, quarter)`:
   ```sql
   UNIQUE(patientuuid, quarter)
   ```

2. **Required Columns**:
   - `patientuuid` (UUID or String) - Patient identifier
   - `quarter` (String) - Quarter value (e.g., 'Q1-2024')
   - `touchtime` (TIMESTAMP) - Last modification timestamp

3. **Example Table Creation**:
   ```sql
   CREATE TABLE patient_quarterly_data (
       recordid SERIAL PRIMARY KEY,
       patientuuid UUID NOT NULL,
       quarter VARCHAR(10) NOT NULL,
       touchtime TIMESTAMP NOT NULL,
       data_field_1 VARCHAR(255),
       data_field_2 VARCHAR(255),
       archived_on TIMESTAMP,
       created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       UNIQUE(patientuuid, quarter)
   );
   ```

## Error Handling

### Connection Errors

```python
import logging

logging.basicConfig(
    filename='quarter_upsert.log',
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

try:
    result = postgresquarterupsert.batch_upsert_by_quarter(conn, table, records)
except Exception as e:
    logging.error(f"Upsert failed: {e}")
    # Handle error
```

### Validation Errors

Records are automatically validated for:
- Missing `patientuuid` → Skipped
- Missing `quarter` → Skipped
- Missing `touchtime` (will use None) → Handled gracefully

### Logging

Operations log to:
- **Console**: Real-time feedback
- **Log File**: `postgresql_errors.log` (errors) and `quarter_upsert.log` (all operations)

## Performance Considerations

### Batch Processing
- Use batch operations for 100+ records
- Batch size sweet spot: 1,000-10,000 records per call
- Larger batches = better throughput but higher memory usage

### Indexes
For optimal performance, create indexes:
```sql
CREATE INDEX idx_patientuuid_quarter ON patient_quarterly_data(patientuuid, quarter);
CREATE INDEX idx_touchtime ON patient_quarterly_data(touchtime);
CREATE INDEX idx_quarter ON patient_quarterly_data(quarter);
```

### Connection Pooling
For high-volume operations, consider connection pooling:
```python
from psycopg2 import pool

connection_pool = pool.SimpleConnectionPool(
    1, 20,
    user=config.DB_USER,
    password=config.DB_PASS,
    host=config.DB_HOST,
    port=config.DB_PORT,
    database=config.DB_NAME
)

conn = connection_pool.getconn()
# ... perform operations ...
connection_pool.putconn(conn)
```

## Examples

See `examples_quarter_upsert.py` for complete working examples including:
- Batch upsert
- Single record upsert  
- Pre-check existing records
- Process MongoDB export
- Quarter range retrieval
- Record deletion

## Troubleshooting

### Issue: "UNIQUE constraint violated"
**Cause**: Composite unique constraint on (patientuuid, quarter) is missing
**Solution**: Ensure your table has:
```sql
UNIQUE(patientuuid, quarter)
```

### Issue: Records not updating
**Cause**: `touchtime` comparison indicates existing record is newer
**Solution**: Check the `touchtime` values and ensure new records have correct timestamps

### Issue: Slow performance with large batches
**Cause**: Batch size too large or missing indexes
**Solution**: 
- Reduce batch size to 5,000-10,000 records
- Add indexes: `CREATE INDEX idx_patientuuid_quarter ON table(patientuuid, quarter);`

### Issue: Connection timeout
**Cause**: Long-running transaction or connection pool exhaustion
**Solution**:
- Break large operations into smaller batches
- Use connection pooling
- Increase PostgreSQL timeout settings

## Integration with Existing ETL

To integrate with your existing ETL scripts:

```python
# In your ETL export script
from dao import postgresdao
from dao import postgresquarterupsert

# After extracting data from MongoDB
extracted_records = extract_from_mongo()  # Your existing function

# Prepare for quarter upsert
conn = postgresdao.connect_to_postgresqldb()

result = postgresquarterupsert.batch_upsert_by_quarter(
    conn,
    'your_table_name',
    extracted_records
)

print(f"ETL completed: {result['inserted']} new, {result['updated']} updated")
conn.close()
```

## License & Support

For issues or questions, check the logs and error messages. All operations return detailed feedback.

