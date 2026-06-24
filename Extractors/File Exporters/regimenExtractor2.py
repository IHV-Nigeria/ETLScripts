from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import Extractors as regimenExport2

start_time = datetime.now()
print ("Start time: ", start_time )


regimenExport2.export_regimen_change(cutoff_datetime=None, filename="regimenExport_Output_" + datetime.now().strftime("%Y_%m_%d_%H_%M_%S") + ".csv")


end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
