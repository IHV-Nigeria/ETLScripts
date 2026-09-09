from datetime import datetime
import sys
import os

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import etl.CSV_FIle_Exporters.First5VL as vlExporter


start_time = datetime.now()
print ("Start time: ", start_time )

# cutoff_date = datetime(2025, 12, 31, 23, 59, 59)

vlExporter.export_aspire_vl_data(cutoff_datetime=None, filename="ASPIRE_VLLineList_Output_" + datetime.now().strftime(
    "%Y_%m_%d_%H_%M_%S") + ".csv")

# vlExporter.export_gf_vl_data(cutoff_datetime=None, filename="GF_VLLineList_Output_" + datetime.now().strftime(
#     "%Y_%m_%d_%H_%M_%S") + ".csv")

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
