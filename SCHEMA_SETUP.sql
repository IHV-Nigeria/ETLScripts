-- PostgreSQL Schema Setup Guide for Quarter-Based Upsert
-- This file contains SQL schemas and index recommendations

-- ============================================================================
-- BASIC TABLE SCHEMA
-- ============================================================================
-- Use this as a template for your actual table
CREATE TABLE patient_quarterly_data (
    recordid SERIAL PRIMARY KEY,
    patientuuid VARCHAR(100) NOT NULL,
    quarter VARCHAR(10) NOT NULL,
    touchtime TIMESTAMP,
    -- Add your custom columns below
    patient_name VARCHAR(255),
    status VARCHAR(50),
    score NUMERIC,
    notes TEXT,
    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- CRITICAL: Composite unique constraint
    UNIQUE(patientuuid, quarter)
);

-- ============================================================================
-- RECOMMENDED INDEXES (for performance optimization)
-- ============================================================================
-- Primary composite key index (created with UNIQUE constraint, but good to list)
CREATE INDEX idx_patientuuid_quarter ON patient_quarterly_data(patientuuid, quarter);

-- Search by patientuuid alone
CREATE INDEX idx_patientuuid ON patient_quarterly_data(patientuuid);

-- Search by quarter alone
CREATE INDEX idx_quarter ON patient_quarterly_data(quarter);

-- Touchtime comparisons (important for upsert logic)
CREATE INDEX idx_touchtime ON patient_quarterly_data(touchtime);

-- Combined index for filtering by quarter and touchtime
CREATE INDEX idx_quarter_touchtime ON patient_quarterly_data(quarter, touchtime);

-- ============================================================================
-- MULTI-TABLE EXAMPLE: Patient Data Across Multiple Quarters
-- ============================================================================
-- If you're tracking different types of data per quarter

-- EAC (Early Adherence Counseling) Quarterly Data
CREATE TABLE eac_quarterly_data (
    recordid SERIAL PRIMARY KEY,
    patientuuid VARCHAR(100) NOT NULL,
    quarter VARCHAR(10) NOT NULL,
    touchtime TIMESTAMP,
    eac_visits_count INT,
    adherence_score NUMERIC,
    intervention_type VARCHAR(100),
    notes TEXT,
    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(patientuuid, quarter)
);

-- ART (Antiretroviral Therapy) Quarterly Data
CREATE TABLE art_quarterly_data (
    recordid SERIAL PRIMARY KEY,
    patientuuid VARCHAR(100) NOT NULL,
    quarter VARCHAR(10) NOT NULL,
    touchtime TIMESTAMP,
    art_regimen VARCHAR(100),
    refill_count INT,
    viral_load NUMERIC,
    cd4_count INT,
    notes TEXT,
    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(patientuuid, quarter)
);

-- Lab Results by Quarter
CREATE TABLE lab_quarterly_data (
    recordid SERIAL PRIMARY KEY,
    patientuuid VARCHAR(100) NOT NULL,
    quarter VARCHAR(10) NOT NULL,
    touchtime TIMESTAMP,
    test_type VARCHAR(100),
    result_value NUMERIC,
    result_unit VARCHAR(50),
    reference_range VARCHAR(100),
    notes TEXT,
    created_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(patientuuid, quarter, test_type)  -- Triple key if multiple tests per quarter
);

-- ============================================================================
-- BULK INDEX CREATION FOR PERFORMANCE
-- ============================================================================
CREATE INDEX idx_eac_quarter_touchtime ON eac_quarterly_data(quarter, touchtime);
CREATE INDEX idx_art_quarter_touchtime ON art_quarterly_data(quarter, touchtime);
CREATE INDEX idx_lab_patientuuid ON lab_quarterly_data(patientuuid);

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check unique constraint exists
SELECT constraint_name, constraint_type
FROM information_schema.table_constraints
WHERE table_name = 'patient_quarterly_data'
AND constraint_type = 'UNIQUE';

-- Check indexes
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'patient_quarterly_data'
ORDER BY indexname;

-- Check table structure
\d patient_quarterly_data;

-- ============================================================================
-- USEFUL QUERIES
-- ============================================================================

-- 1. Get latest record for each patient (most recent touchtime)
SELECT DISTINCT ON (patientuuid)
    patientuuid, quarter, touchtime, *
FROM patient_quarterly_data
ORDER BY patientuuid, touchtime DESC;

-- 2. Find duplicate (patientuuid, quarter) - should be 0 after proper setup
SELECT patientuuid, quarter, COUNT(*)
FROM patient_quarterly_data
GROUP BY patientuuid, quarter
HAVING COUNT(*) > 1;

-- 3. Records added today
SELECT *
FROM patient_quarterly_data
WHERE created_on >= CURRENT_DATE;

-- 4. Recently updated records (by touchtime)
SELECT *
FROM patient_quarterly_data
WHERE touchtime >= NOW() - INTERVAL '7 days'
ORDER BY touchtime DESC;

-- 5. Records for specific quarter
SELECT *
FROM patient_quarterly_data
WHERE quarter = 'Q1-2024'
ORDER BY patientuuid;

-- 6. Patients in database
SELECT DISTINCT patientuuid
FROM patient_quarterly_data
ORDER BY patientuuid;

-- 7. Quarter coverage
SELECT DISTINCT quarter
FROM patient_quarterly_data
ORDER BY quarter DESC;

-- 8. Data quality check - records with NULL touchtime
SELECT COUNT(*) as null_touchtime_count
FROM patient_quarterly_data
WHERE touchtime IS NULL;

-- ============================================================================
-- MAINTENANCE QUERIES
-- ============================================================================

-- Remove duplicate records (keep newest by touchtime)
DELETE FROM patient_quarterly_data
WHERE recordid NOT IN (
    SELECT DISTINCT ON (patientuuid, quarter) recordid
    FROM patient_quarterly_data
    ORDER BY patientuuid, quarter, touchtime DESC
);

-- Update modified timestamp when record changes
ALTER TABLE patient_quarterly_data
ADD COLUMN IF NOT EXISTS updated_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Create trigger to auto-update updated_on
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_on = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_timestamp_trigger
BEFORE UPDATE ON patient_quarterly_data
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

-- ============================================================================
-- PERFORMANCE TIPS
-- ============================================================================

-- 1. VACUUM and ANALYZE for optimization
VACUUM ANALYZE patient_quarterly_data;

-- 2. Check table size
SELECT
    pg_size_pretty(pg_total_relation_size('patient_quarterly_data')) as total_size,
    pg_size_pretty(pg_relation_size('patient_quarterly_data')) as table_size,
    pg_size_pretty(pg_total_relation_size('patient_quarterly_data') - pg_relation_size('patient_quarterly_data')) as indexes_size;

-- 3. Query statistics
EXPLAIN ANALYZE
SELECT * FROM patient_quarterly_data
WHERE patientuuid = 'patient-001' AND quarter = 'Q1-2024';

-- ============================================================================
-- BACKUP AND RESTORE
-- ============================================================================

-- Backup single table
-- pg_dump -U postgres -d test -t patient_quarterly_data > patient_quarterly_data_backup.sql

-- Restore table
-- psql -U postgres -d test < patient_quarterly_data_backup.sql

-- Export to CSV
-- COPY patient_quarterly_data TO '/path/to/export.csv' WITH (FORMAT CSV, HEADER);

-- Import from CSV
-- COPY patient_quarterly_data FROM '/path/to/import.csv' WITH (FORMAT CSV, HEADER);

-- ============================================================================
-- ARCHIVE OLD DATA
-- ============================================================================

-- Create archive table with identical structure
CREATE TABLE patient_quarterly_data_archive (LIKE patient_quarterly_data);

-- Move old data to archive (e.g., before 2024)
INSERT INTO patient_quarterly_data_archive
SELECT * FROM patient_quarterly_data
WHERE quarter < 'Q1-2024';

-- Delete from main table
DELETE FROM patient_quarterly_data
WHERE quarter < 'Q1-2024';

-- ============================================================================
-- COMMON ISSUES & SOLUTIONS
-- ============================================================================

-- Issue 1: Constraint violation on insert
-- Solution: Check for existing records with same (patientuuid, quarter)
SELECT * FROM patient_quarterly_data
WHERE patientuuid = 'patient-001' AND quarter = 'Q1-2024';

-- Issue 2: Updates not working as expected
-- Solution: Check touchtime values
SELECT patientuuid, quarter, touchtime
FROM patient_quarterly_data
WHERE patientuuid = 'patient-001' AND quarter = 'Q1-2024'
ORDER BY touchtime DESC;

-- Issue 3: Slow queries
-- Solution: Check query plan
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM patient_quarterly_data
WHERE patientuuid = 'patient-001' AND quarter LIKE 'Q%';

-- Solution 2: Reindex
REINDEX TABLE patient_quarterly_data;

-- ============================================================================
-- SAMPLE DATA FOR TESTING
-- ============================================================================

-- Insert sample records
INSERT INTO patient_quarterly_data
(patientuuid, quarter, touchtime, patient_name, status, score, notes)
VALUES
('patient-001', 'Q1-2024', '2024-01-15 10:30:00', 'John Doe', 'Active', 85.5, 'Initial record'),
('patient-002', 'Q1-2024', '2024-01-16 14:45:00', 'Jane Smith', 'Active', 92.0, 'Q1 data'),
('patient-001', 'Q2-2024', '2024-04-10 09:15:00', 'John Doe', 'Active', 88.0, 'Q2 data'),
('patient-003', 'Q1-2024', '2024-01-20 11:00:00', 'Bob Johnson', 'Inactive', 76.5, 'Limited data');

-- Verify insert
SELECT * FROM patient_quarterly_data;

-- ============================================================================
-- QUARTERLY FILTER PATTERNS
-- ============================================================================

-- Q1-2024, Q2-2024, ... Q4-2026 (ISO week date format)
-- Pattern: Q[1-4]-[YYYY]

-- Query by quarter pattern
SELECT * FROM patient_quarterly_data
WHERE quarter ~ 'Q[1-4]-[0-9]{4}';  -- Regex match

-- Query specific year
SELECT * FROM patient_quarterly_data
WHERE quarter LIKE '%2024';

-- Query range (lexicographic comparison)
SELECT * FROM patient_quarterly_data
WHERE quarter >= 'Q1-2024' AND quarter <= 'Q4-2024';

-- List all quarters in database
SELECT DISTINCT quarter
FROM patient_quarterly_data
ORDER BY quarter DESC;

