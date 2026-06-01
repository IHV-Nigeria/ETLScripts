from datetime import datetime, timedelta
import sys
import os
import etl.EACDataExport as EACDataExport
import time
from legacy.runSchedular import job

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


start_time = datetime.now()
print ("Start time: ", start_time )

def seconds_until_midnight():
    now = datetime.now()
    tomorrow = now + timedelta(hours=1)
    midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day)
    return (midnight - now).total_seconds()

# cutoff_date = datetime(2024, 10, 1, 0, 0, 0)

# EACDataExport.export_eac_data(cutoff_datetime=None, filename="EACDataExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv")

# print("Running Initial line list extraction...")

EACDataExport.initialize_eac_line_list_data(cutoff_datetime=None)

print("Starting upsert function...")
while True:
    # time.sleep(timedelta(minutes=15).total_seconds())
    time.sleep(seconds_until_midnight())
    print("Job Time: ", datetime.now())
    job(lambda: EACDataExport.upsert_art_line_list_data(cutoff_datetime=None))


end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
