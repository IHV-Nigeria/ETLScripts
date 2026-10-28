from datetime import datetime
import sys
import os

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import etl.CSV_FIle_Exporters.s_uruakpa as extract_data


start_time = datetime.now()
print ("Start time: ", start_time )

cutoff_date1 = datetime(2021, 12, 31, 23, 59, 59)

cutoff_date2 = datetime(2022, 12, 31, 23, 59, 59)

cutoff_date3 = datetime(2025, 12, 31, 23, 59, 59)

extract_data.export_data(cutoff_datetime=cutoff_date1, filename="s_uruakpa_LineList_Output_" + datetime.now().strftime(
    "%Y_%m_%d_%H_%M_%S") + ".csv")

extract_data.export_data(cutoff_datetime=cutoff_date2, filename="s_uruakpa_LineList_Output_" + datetime.now().strftime(
    "%Y_%m_%d_%H_%M_%S") + ".csv")

extract_data.export_data(cutoff_datetime=cutoff_date3, filename="s_uruakpa_LineList_Output_" + datetime.now().strftime(
    "%Y_%m_%d_%H_%M_%S") + ".csv")

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
