-- One-time migration for ART line list upsert key.
-- Run this script once per database before running ART upserts.

BEGIN;

-- Remove legacy uniqueness that only keyed on patientuuid.
ALTER TABLE art_line_list
DROP CONSTRAINT IF EXISTS art_line_list_patientuuid_key;

-- Ensure the upsert conflict target exists.
CREATE UNIQUE INDEX IF NOT EXISTS ux_art_line_list_patientuuid_datimcode
ON art_line_list (patientuuid, datimcode);

COMMIT;
