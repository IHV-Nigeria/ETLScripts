from datetime import datetime, date

import sys
import os

# Adds the parent directory to the python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


import etl.EACDataExport as EACDataExport
import etl.EACDataExportMultiprocess as EACDataExportMultiprocess

import etl.ViralLoadAndIITPattern as ViralLoadAndIITPattern
import etl.TBOutcomeStudy as TBOutcomeStudy
import etl.IITEpisodeExport as IITEpisodeExport
import etl.CDRLineList as CDRLineList


start_time = datetime.now()
print ("Start time: ", start_time )

#EACDataExport.export_eac_data(filename="EACDataExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv")
#ViralLoadAndIITPattern.export_iit_vl_data(filename="ViralLoadAndIITPattern_Output_"+datetime.now().strftime("_%Y_%m_%d %H_%M_%S")+".csv")
#EACDataExportMultiprocess.producer_consumer_etl(filename="EACDataExportMultiprocess_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv")
#TBOutcomeStudy.export_tb_outcome_study_data(filename="TBOutcomeStudy_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv") 
cutoff_date = datetime(2025, 12, 31, 23, 59, 59)
IITEpisodeExport.export_iit_episode_data(
      patient_baseline_file="PatientBaselineExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv",
      iit_episode_file="IITEpisodeExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv",
      cutoff_datetime=cutoff_date
      
      )

#CDRLineList.export_cdr_line_list_data(filename="CDRLineList_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv", cutoff_datetime=cutoff_date)

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
