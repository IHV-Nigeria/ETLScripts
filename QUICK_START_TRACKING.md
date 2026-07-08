# Quick Start: EAC Export Tracking

## 🚀 Get Started in 2 Minutes

### What You Get

When you run the EAC export, you'll automatically get **3 CSV files** tracking:
- ✅ **New records** (datim_code, uniqueID, touchtime)
- ✅ **Updated records** (datim_code, uniqueID, old_touchtime → new_touchtime)  
- ✅ **Errors** (datim_code, uniqueID, error_message)

### Run the Export

**Option 1: Direct Python**
```python
from etl.Postgres_Export.eacPgExport import export_eac_data_to_postgresql

result = export_eac_data_to_postgresql()
print(f"CSV Files Generated - Inserted: {result['csv_files']['inserted_count']}, Updated: {result['csv_files']['updated_count']}")
```

**Option 2: Via Scheduler**
```bash
python Extractors/Postgre\ Importers/EACpgExtractor.py
```

The script will:
1. Run the export
2. Generate CSV files in `./upsert_logs/`
3. Log the counts to console and log files

### Find Your Files

Look in `./upsert_logs/` directory:
```
upsert_logs/
├── eac_inserted_20240708_121530.csv   ← New records
├── eac_updated_20240708_121530.csv    ← Updated records
└── eac_errors_20240708_121530.csv     ← Errors (if any)
```

### Open in Excel

1. Open Excel
2. File → Open → Navigate to `./upsert_logs/`
3. Select any CSV file → Open
4. Excel auto-formats the data
5. Done! Now you can analyze

### Quick Analysis

**Python (if you have pandas):**
```python
import pandas as pd

# Check how many records were inserted
inserted = pd.read_csv('./upsert_logs/eac_inserted_20240708_121530.csv')
print(f"Total inserted: {len(inserted)}")
print(f"By facility:")
print(inserted.groupby('datim_code').size())
```

## CSV Column Reference

### `eac_inserted_*.csv`
| Column | Example |
|--------|---------|
| patientuuid | 550e8400-e29b-41d4-a716-446655440000 |
| datim_code | LmLBtmd8U43 |
| uniqueID | MRN-12345 |
| quarter | Q3-2024 |
| touchtime | 2024-07-08 11:45:00 |

### `eac_updated_*.csv`
| Column | Example |
|--------|---------|
| patientuuid | 550e8400-e29b-41d4-a716-446655440000 |
| datim_code | LmLBtmd8U43 |
| uniqueID | MRN-12345 |
| quarter | Q3-2024 |
| old_touchtime | 2024-07-01 10:30:00 |
| new_touchtime | 2024-07-08 14:25:30 |

### `eac_errors_*.csv`
| Column | Example |
|--------|---------|
| patientuuid | 550e8400-e29b-41d4-a716-446655440000 |
| datim_code | LmLBtmd8U43 |
| uniqueID | MRN-12345 |
| error | Connection timeout during database operation |

## Common Questions

**Q: Will my existing code break?**
A: No! CSV generation is completely automatic and doesn't affect existing functionality.

**Q: How often are these generated?**
A: Every time you run the export. Each run gets a unique timestamp.

**Q: Can I delete old CSV files?**
A: Yes! They're just for tracking. Archive or delete as needed.

**Q: Why is a file empty?**
A: If no records were inserted/updated/errored, that CSV won't be created.

**Q: How do I email these files?**
A: Add this after export completes:
```python
import os
import glob

# Find the latest CSV files
csvs = glob.glob('./upsert_logs/eac_*.csv')
# Then send via email API...
```

## What Changed

✅ Fixed earlier cursor unpacking bugs across all files  
✅ Added tracking to capture insert/update/error details  
✅ Generates 3 CSV files automatically in `./upsert_logs/`
✅ Logs summary to console  
✅ No code changes needed to use

## Need Help?

See:
- **Full Documentation**: `EAC_EXPORT_TRACKING_GUIDE.md`
- **Examples**: `EAC_CSV_EXAMPLES.md`
- **Implementation Details**: `TRACKING_IMPLEMENTATION_SUMMARY.md`


