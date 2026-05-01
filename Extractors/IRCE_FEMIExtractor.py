from datetime import datetime, date
import sys
import os
import etl.IRCE_FEMIExport as IRCE_FEMIExport

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

start_time = datetime.now()
print ("Start time: ", start_time )

#cutoff_date = datetime(2024, 10, 1, 0, 0, 0)

IRCE_FEMIExport.export_request_data(cutoff_datetime=None, filename="IRCE_DATA_REQUEST_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv")

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
