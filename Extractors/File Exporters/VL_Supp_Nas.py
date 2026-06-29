from datetime import datetime
import sys
import os
import etl.CSV_FIle_Exporters.VLSuppression_Nas as VLSuppression_Nas

import importlib.util

start_time = datetime.now()
print ("Start time: ", start_time )

cutoff_date = None #datetime(2025, 12, 31, 23, 59, 59)

VLSuppression_Nas.export_data(cutoff_datetime=cutoff_date)

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
