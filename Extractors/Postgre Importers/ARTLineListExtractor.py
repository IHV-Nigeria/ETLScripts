from datetime import datetime, date

import sys
import os

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import etl.ARTLineList as ARTLineList


start_time = datetime.now()
print ("Start time: ", start_time )

cutoff_date = datetime(2025, 12, 31, 23, 59, 59)

# ARTLineList.initialize_art_line_list_data()

ARTLineList.upsert_art_line_list_data()

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )