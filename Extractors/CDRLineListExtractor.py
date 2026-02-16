from datetime import datetime, date

import sys
import os

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from etl import EACDataExport 


import etl.CDRLineList as CDRLineList


start_time = datetime.now()
print ("Start time: ", start_time )

cutoff_date = datetime(2025, 12, 31, 23, 59, 59)

CDRLineList.export_cdr_line_list_data(filename="CDRLineList_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv", cutoff_datetime=cutoff_date)

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
