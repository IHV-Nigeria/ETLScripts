from datetime import datetime
import etl.TBOutcomeStudy as TBOutcomeStudy

start_time = datetime.now()
print ("Start time: ", start_time )

TBOutcomeStudy.export_tb_outcome_study_data(filename="TBOutcomeStudy_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv")

cutoff_date = datetime(2025, 12, 31, 23, 59, 59)

TBOutcomeStudy.export_tb_outcome_study_data(
    cutoff_datetime=cutoff_date,
    filename="TBOutcomeStudyExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv"
)

end_time = datetime.now()
print ("End time: ", end_time )
duration = end_time - start_time
print ("Duration: ", duration )
