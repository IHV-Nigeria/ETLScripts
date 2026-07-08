# EAC Export CSV Format Reference

## Example CSV Files

All files are stored in `./upsert_logs/`

### 1. Inserted Records Example
**Filename:** `eac_inserted_20240708_114530.csv`

```csv
patientuuid,datim_code,uniqueID,quarter,touchtime
550e8400-e29b-41d4-a716-446655440000,LmLBtmd8U43,MRN-00001,Q3-2024,2024-07-08 11:45:00
550e8400-e29b-41d4-a716-446655440001,sQsPw6L5k3g,MRN-00002,Q3-2024,2024-07-08 11:45:05
550e8400-e29b-41d4-a716-446655440002,Hzk9vLxQ2Wp,MRN-00003,Q3-2024,2024-07-08 11:45:10
550e8400-e29b-41d4-a716-446655440003,kD3rPqMn8Yx,MRN-00004,Q3-2024,2024-07-08 11:45:15
550e8400-e29b-41d4-a716-446655440004,tWx4BcV6jNp,MRN-00005,Q3-2024,2024-07-08 11:45:20
```

**Columns:**
- `patientuuid`: Unique patient identifier in the system
- `datim_code`: Facility's DATIM code (e.g., state/facility identifier)
- `uniqueID`: Patient's medical record number or unique identifier
- `quarter`: Quarter when record became active (format: Q#-YYYY)
- `touchtime`: ISO timestamp of when record was inserted

---

### 2. Updated Records Example
**Filename:** `eac_updated_20240708_114530.csv`

```csv
patientuuid,datim_code,uniqueID,quarter,old_touchtime,new_touchtime
550e8400-e29b-41d4-a716-446655440005,LmLBtmd8U43,MRN-00006,Q3-2024,2024-07-01 10:30:00,2024-07-08 14:25:30
550e8400-e29b-41d4-a716-446655440006,sQsPw6L5k3g,MRN-00007,Q3-2024,2024-07-05 09:15:45,2024-07-08 14:25:35
550e8400-e29b-41d4-a716-446655440007,Hzk9vLxQ2Wp,MRN-00008,Q2-2024,2024-06-30 16:45:00,2024-07-08 14:25:40
550e8400-e29b-41d4-a716-446655440008,kD3rPqMn8Yx,MRN-00009,Q3-2024,2024-07-03 11:20:15,2024-07-08 14:25:45
550e8400-e29b-41d4-a716-446655440009,tWx4BcV6jNp,MRN-00010,Q3-2024,2024-07-06 13:50:30,2024-07-08 14:25:50
```

**Columns:**
- `patientuuid`: Unique patient identifier
- `datim_code`: Facility's DATIM code
- `uniqueID`: Patient's medical record number
- `quarter`: Quarter for the record
- `old_touchtime`: Previous timestamp before update
- `new_touchtime`: New timestamp after update

**Usage:** Can calculate time delta: `new_touchtime - old_touchtime` to see how long between updates

---

### 3. Error Records Example
**Filename:** `eac_errors_20240708_114530.csv`

```csv
patientuuid,datim_code,uniqueID,error
550e8400-e29b-41d4-a716-446655440010,LmLBtmd8U43,MRN-00011,"Missing required field: facilityDatimCode"
550e8400-e29b-41d4-a716-446655440011,sQsPw6L5k3g,MRN-00012,"Invalid quarter format"
550e8400-e29b-41d4-a716-446655440012,,MRN-00013,"Cannot extract patient UUID from document"
,Hzk9vLxQ2Wp,,DATIM code not found in facility cache - invalid facility
550e8400-e29b-41d4-a716-446655440013,kD3rPqMn8Yx,MRN-00014,"Connection timeout during database operation"
```

**Columns:**
- `patientuuid`: Patient UUID (may be empty if error occurred before extraction)
- `datim_code`: Facility code (may be empty if facility lookup failed)
- `uniqueID`: Patient's medical record number (may be empty if error occurred early)
- `error`: Human-readable error message explaining what went wrong

**Notes:**
- Some fields may be empty (N/A) if error occurred before those fields could be extracted
- Error messages help identify data quality issues or system problems

---

## CSV Analysis Examples

### Count Records by Facility

```python
import pandas as pd

# Read inserted records
inserted = pd.read_csv('./upsert_logs/eac_inserted_20240708_114530.csv')

# Count by DATIM code
facility_counts = inserted.groupby('datim_code').size()
print(facility_counts)
# Output:
# datim_code
# Hzk9vLxQ2Wp    1
# LmLBtmd8U43    1
# kD3rPqMn8Yx    1
# sQsPw6L5k3g    1
# tWx4BcV6jNp    1
# dtype: int64
```

### Calculate Update Frequency

```python
updated = pd.read_csv('./upsert_logs/eac_updated_20240708_114530.csv')

# Convert timestamps to datetime
updated['old_touchtime'] = pd.to_datetime(updated['old_touchtime'])
updated['new_touchtime'] = pd.to_datetime(updated['new_touchtime'])

# Calculate days since last update
updated['days_since_update'] = (updated['new_touchtime'] - updated['old_touchtime']).dt.days

print(updated[['uniqueID', 'days_since_update']])
```

### Error Analysis

```python
errors = pd.read_csv('./upsert_logs/eac_errors_20240708_114530.csv')

# Count errors by type
print(errors['error'].value_counts())

# Filter by facility
facility_errors = errors[errors['datim_code'] == 'LmLBtmd8U43']
print(f"Errors at LmLBtmd8U43: {len(facility_errors)}")
```

---

## File Organization Example

```
upsert_logs/
├── 2024-07-08/
│   ├── eac_inserted_20240708_000100.csv    (Midnight run)
│   └── eac_updated_20240708_000100.csv
│
├── 2024-07-09/
│   ├── eac_inserted_20240709_000100.csv    (Next day)
│   ├── eac_updated_20240709_000100.csv
│   └── eac_errors_20240709_000100.csv
│
└── archive/
    └── 2024-07-01-to-07-07.zip             (Older exports)
```

---

## Integration with External Tools

### SQL Server Import
```sql
BULK INSERT staging.eac_inserted
FROM 'C:\upsert_logs\eac_inserted_20240708_114530.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);
```

### Excel Analysis
1. Open Excel → Data → From Text/CSV
2. Select CSV file from `./upsert_logs/`
3. Configure delimiters (comma)
4. Create pivot tables for analysis

### Power BI Dashboard
1. Connect to `./upsert_logs/` folder
2. Configure automatic refresh
3. Create reports for tracking metrics

---

## Performance Metrics

**Typical CSV File Sizes:**
- 10,000 inserted records ≈ 1.2 MB
- 5,000 updated records ≈ 0.8 MB
- 100 error records ≈ 0.1 MB

**Generation Time:**
- CSV writing adds < 5 seconds to 10,000 record batch
- Database upsert is primary bottleneck, not CSV writing


