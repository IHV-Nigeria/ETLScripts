from datetime import datetime
import sys
import os
import etl.CSV_FIle_Exporters.VLSuppression_Nas as VLSuppression_Nas

import importlib.util

start_time = datetime.now()
print ("Start time: ", start_time )

cutoff_date = None #datetime(2025, 12, 31, 23, 59, 59)

VLSuppression_Nas.export_data(filename="NasVL_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv", cutoff_datetime=None)

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
