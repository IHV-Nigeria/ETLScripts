# EAC Export Tracking and Logging Guide

## Overview

The EAC PostgreSQL export pipeline has been enhanced to generate detailed tracking CSV files for all record operations. This provides a comprehensive audit trail of:
- **Newly inserted records** (DatimCode, UniqueID, timestamp)
- **Updated records** (DatimCode, UniqueID, previous & new timestamps)
- **Error records** (DatimCode, UniqueID, error details)

## CSV Output Files

All CSV files are generated in the `./upsert_logs/` directory with timestamps in the filename.

### 1. Inserted Records: `eac_inserted_YYYYMMDD_HHMMSS.csv`

Contains all newly inserted records with the following columns:

| Column | Description |
|--------|-------------|
| `patientuuid` | Patient UUID from the document |
| `datim_code` | Facility DATIM code |
| `uniqueID` | Patient's unique identifier |
| `quarter` | Quarter value for grouping |
| `touchtime` | Timestamp when record was inserted |

**Example:**
```
patientuuid,datim_code,uniqueID,quarter,touchtime
550e8400-e29b-41d4-a716-446655440000,LmLBtmd8U43,12345,Q3-2024,2024-07-08 11:45:00
```

### 2. Updated Records: `eac_updated_YYYYMMDD_HHMMSS.csv`

Contains all updated records with comparison of old and new touch times:

| Column | Description |
|--------|-------------|
| `patientuuid` | Patient UUID from the document |
| `datim_code` | Facility DATIM code |
| `uniqueID` | Patient's unique identifier |
| `quarter` | Quarter value for grouping |
| `old_touchtime` | Previous timestamp in database |
| `new_touchtime` | New timestamp after update |

**Example:**
```
patientuuid,datim_code,uniqueID,quarter,old_touchtime,new_touchtime
550e8400-e29b-41d4-a716-446655440001,LmLBtmd8U43,12346,Q3-2024,2024-07-01 10:30:00,2024-07-08 11:45:00
```

### 3. Error Records: `eac_errors_YYYYMMDD_HHMMSS.csv`

Contains records that failed during processing:

| Column | Description |
|--------|-------------|
| `patientuuid` | Patient UUID (if available) |
| `datim_code` | Facility DATIM code (if available) |
| `uniqueID` | Patient's unique identifier (if available) |
| `error` | Error message describing what went wrong |

**Example:**
```
patientuuid,datim_code,uniqueID,error
550e8400-e29b-41d4-a716-446655440002,LmLBtmd8U43,12347,"Connection timeout"
```

## Implementation Details

### Modified Files

1. **`dao/postgresquarterupsert.py`**
   - Added new function: `batch_upsert_by_quarter_with_tracking()`
   - Tracks all inserts, updates, and errors with detailed metadata
   - Returns both counts and detailed change information

2. **`etl/Postgres_Export/eacPgExport.py`**
   - Updated `export_eac_data_to_postgresql()` to use tracking function
   - Added `_write_csv_file()` helper function for CSV generation
   - Collects tracking data from all batches
   - Writes CSV files at end of export to `./upsert_logs/`
   - Returns export results with CSV file counts

3. **`Extractors/Postgre Importers/EACpgExtractor.py`**
   - Updated to display CSV file generation information in logs
   - Shows count of inserted, updated, and error records

### Key Features

✅ **Detailed Audit Trail**: Every record operation is tracked and logged to CSV  
✅ **Touch Time Comparison**: Updated records show before/after timestamps  
✅ **Error Tracking**: Failed records are captured with error messages  
✅ **Batched Processing**: Tracking works with large-scale batch operations  
✅ **Timestamped Files**: Each export run generates uniquely timestamped files  
✅ **Safe Writes**: CSV files are only written if there's data to record  

## Usage

### Automatic Generation

The CSV files are automatically generated when you run the export:

```python
from etl.Postgres_Export.eacPgExport import export_eac_data_to_postgresql

result = export_eac_data_to_postgresql()

# Check how many records were tracked
print(f"Inserted: {result['csv_files']['inserted_count']}")
print(f"Updated: {result['csv_files']['updated_count']}")
print(f"Errors: {result['csv_files']['error_count']}")
```

### Log Output

The export logs will show:

```
[TRACKING] Inserted records: 1250
[TRACKING] Updated records: 450
[TRACKING] Error records: 5
```

### File Location

All CSV files are stored in: `./upsert_logs/`

```
upsert_logs/
├── eac_inserted_20240708_114530.csv
├── eac_updated_20240708_114530.csv
└── eac_errors_20240708_114530.csv
```

## Data Retention

- CSV files are cumulative (not overwritten)
- Each export run generates new dated files
- Files can be moved/archived as needed
- Timestamp in filename allows tracking over time

## Integration with Scheduler

When using `EACpgExtractor.py`, tracking information is automatically logged:

```
[TRACKING] CSV files generated:
  - Inserted records: 1250
  - Updated records: 450
  - Error records: 5
```

## Debugging and Analysis

The CSV files can be easily analyzed:

```python
import pandas as pd

# Analyze inserted records
inserted = pd.read_csv('./upsert_logs/eac_inserted_20240708_114530.csv')
print(f"Total inserted: {len(inserted)}")
print(inserted.groupby('datim_code').size())

# Analyze updated records
updated = pd.read_csv('./upsert_logs/eac_updated_20240708_114530.csv')
print(f"Total updated: {len(updated)}")

# Analyze errors
errors = pd.read_csv('./upsert_logs/eac_errors_20240708_114530.csv')
print(f"Error details:\n{errors}")
```

## Performance Considerations

- Tracking adds minimal overhead (field extraction only)
- CSV writing happens after all batches are processed
- Memory efficient: details are collected in lists, not entire records
- Batch processing continues regardless of CSV writing success

## Troubleshooting

**Q: CSV files not being generated?**
- Check `./upsert_logs/` directory exists (created automatically if needed)
- Check log files for write errors
- Verify disk space availability

**Q: Missing data in CSV files?**
- Ensure `datimcode` and `uniqueid` columns exist in documents
- Check for None/NULL values in source data
- Review error logs for data validation issues

**Q: File size concerns?**
- Large exports generate large CSV files
- Archive old files periodically
- Consider compressing historical exports

## Future Enhancements

Possible future improvements:
- Batch CSV files by facility (DATIM code)
- Generate summary statistics reports
- Email notifications of errors
- Database logging as alternative to CSV


