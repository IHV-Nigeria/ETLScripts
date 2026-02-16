from datetime import datetime, date
import etl.EACDataExport as EACDataExport
import etl.EACDataExportMultiprocess as EACDataExportMultiprocess

import etl.ViralLoadAndIITPattern as ViralLoadAndIITPattern
import etl.TBOutcomeStudy as TBOutcomeStudy
import etl.IITEpisodeExport as IITEpisodeExport


start_time = datetime.now()
print ("Start time: ", start_time )

TBOutcomeStudy.export_tb_outcome_study_data(filename="TBOutcomeStudy_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv")

cutoff_date = datetime(2025, 12, 31, 23, 59, 59)

TBOutcomeStudy.export_iit_episode_data(
    patient_baseline_file="PatientBaselineExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv",
    iit_episode_file="TBOutcomeStudyExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv",
    cutoff_datetime=cutoff_date

)

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
