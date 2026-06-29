from datetime import datetime, timedelta
import sys
import os
import time
import logging

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('eac_pg_extractor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import the upsert function
try:
    from etl.Postgres_Export.eacPgExport import export_eac_data_to_postgresql
    logger.info("[OK] Successfully imported export_eac_data_to_postgresql")
except ImportError as e:
    logger.error(f"[ERROR] Failed to import export_eac_data_to_postgresql: {e}")
    sys.exit(1)

# Import scheduler
try:
    from legacy.runSchedular import job
    logger.info("[OK] Successfully imported job scheduler")
except ImportError as e:
    logger.error(f"[ERROR] Failed to import job scheduler: {e}")
    sys.exit(1)

start_time = datetime.now()
logger.info(f"EAC PostgreSQL Extractor started at: {start_time}")


def seconds_until_midnight():
    """Calculate seconds until midnight"""
    now = datetime.now()
    next_midnight = datetime.combine(
        now.date() + timedelta(days=1),
        datetime.min.time()
    )
    return max(0, int((next_midnight - now).total_seconds()))


def countdown_to_midnight():
    """Display countdown timer until midnight"""
    while True:
        seconds = seconds_until_midnight()

        if seconds <= 0:
            break

        hours, remainder = divmod(seconds, 3600)
        mins, secs = divmod(remainder, 60)

        sys.stdout.write(
            f"\r[WAIT] Time until next run: {hours:02d}:{mins:02d}:{secs:02d}"
        )
        sys.stdout.flush()

        time.sleep(1)

    logger.info("[RUN] Midnight reached! Running export job...")
    print("\n[RUN] Midnight reached! Running job...")


# =============================================================================
# INITIAL EXPORT
# =============================================================================

logger.info("=" * 80)
logger.info("PERFORMING INITIAL EAC DATA EXPORT")
logger.info("=" * 80)

try:
    logger.info("Starting upsert process...")
    result = export_eac_data_to_postgresql(cutoff_datetime=None)
    
    logger.info(f"Initial export completed:")
    logger.info(f"  [OK] Inserted: {result.get('inserted', 0)}")
    logger.info(f"  [OK] Updated: {result.get('updated', 0)}")
    logger.info(f"  [OK] Skipped: {result.get('skipped', 0)}")
    logger.info(f"  [ERROR] Errors: {result.get('errors', 0)}")
    
except Exception as e:
    logger.error(f"[ERROR] Initial export failed: {e}", exc_info=True)
    sys.exit(1)


# =============================================================================
# MAIN SCHEDULER LOOP
# =============================================================================

logger.info("=" * 80)
logger.info("STARTING SCHEDULER - Waiting for midnight...")
logger.info("=" * 80)

while True:
    try:
        countdown_to_midnight()

        logger.info("=" * 80)
        logger.info(f"SCHEDULED JOB STARTED at {datetime.now()}")
        logger.info("=" * 80)

        start = time.time()

        # Run the export
        result = export_eac_data_to_postgresql(cutoff_datetime=None)
        
        end = time.time()
        duration = int(end - start)

        logger.info(f"Scheduled export completed in {duration} seconds:")
        logger.info(f"  [OK] Inserted: {result.get('inserted', 0)}")
        logger.info(f"  [OK] Updated: {result.get('updated', 0)}")
        logger.info(f"  [OK] Skipped: {result.get('skipped', 0)}")
        logger.info(f"  [ERROR] Errors: {result.get('errors', 0)}")
        logger.info(f"[OK] Job completed successfully in {duration} seconds")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"[ERROR] Scheduled job failed: {e}", exc_info=True)
        logger.error("Job will retry at next scheduled time")
        # Continue to next iteration instead of crashing
        continue
