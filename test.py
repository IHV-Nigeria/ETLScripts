from datetime import datetime, date
import etl.EACDataExport as EACDataExport
import etl.ViralLoadAndIITPattern as ViralLoadAndIITPattern

start_time = datetime.now()
print ("Start time: ", start_time )

# EACDataExport.export_eac_data(filename="EACDataExport_Output_"+datetime.now().strftime("_%Y_%m_%d %H_%M_%S")+".csv")
ViralLoadAndIITPattern.export_iit_vl_data(filename="ViralLoadAndIITPattern_Output_"+datetime.now().strftime("_%Y_%m_%d %H_%M_%S")+".csv")
end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
