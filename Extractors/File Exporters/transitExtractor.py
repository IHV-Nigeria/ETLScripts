from datetime import datetime
import sys
import os
import etl.transitDataExport as transitDataExport

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


start_time = datetime.now()
print ("Start time: ", start_time )

cutoff_date = datetime(2025, 12, 31, 23, 59, 59)

transitDataExport.export_transit_line_list_data(filename="transitLineList_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv", cutoff_datetime=None)

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
