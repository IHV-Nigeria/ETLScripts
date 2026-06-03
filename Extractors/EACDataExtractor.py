from datetime import datetime, timedelta
import sys
import os
import etl.EACDataExport as EACDataExport
from legacy.runSchedular import job
import time

# Add parent directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

start_time = datetime.now()
print("Start time:", start_time)


def seconds_until_midnight():
    now = datetime.now()
    next_midnight = datetime.combine(
        now.date() + timedelta(days=1),
        datetime.min.time()
    )
    return max(0, int((next_midnight - now).total_seconds()))



def countdown_to_midnight():
    while True:
        seconds = seconds_until_midnight()

        if seconds <= 0:
            break

        hours, remainder = divmod(seconds, 3600)
        mins, secs = divmod(remainder, 60)

        sys.stdout.write(
            f"\r⏳ Time until next run: {hours:02d}:{mins:02d}:{secs:02d}"
        )
        sys.stdout.flush()

        time.sleep(1)

    print("\n🔥 Midnight reached! Running job...")


# 🔹 Initial load
# EACDataExport.initialize_eac_line_list_data(cutoff_datetime=None)

print("Starting upsert function...")

# 🔹 Main scheduler loop
while True:
    countdown_to_midnight()

    print("Job started at:", datetime.now())

    start = time.time()

    job(lambda: EACDataExport.upsert_art_line_list_data(cutoff_datetime=None))

    end = time.time()

    print(f"✅ Job completed in {int(end - start)} seconds")
