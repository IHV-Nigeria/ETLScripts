from datetime import datetime, date

import sys
import os

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import etl.EACDataExport as EACDataExport


start_time = datetime.now()
print ("Start time: ", start_time )

#cutoff_date = datetime(2024, 10, 1, 0, 0, 0)    

# EACDataExport.export_eac_data(cutoff_datetime=None, filename="EACDataExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv")

# EACDataExport.initialize_eac_line_list_data(cutoff_datetime=None)

EACDataExport.upsert_art_line_list_data(cutoff_datetime=None)


end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
