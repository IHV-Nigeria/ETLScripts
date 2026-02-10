from datetime import datetime, date
import etl.EACDataExport as EACDataExport
import etl.EACDataExportMultiprocess as EACDataExportMultiprocess

import etl.ViralLoadAndIITPattern as ViralLoadAndIITPattern


# This guard is MANDATORY on Windows for Multiprocessing
if __name__ == '__main__':
    # Optional: help Windows handle the executable environment
    # from multiprocessing import freeze_support
    # freeze_support()

    start_time = datetime.now()
    print ("Start time: ", start_time )

    #EACDataExport.export_eac_data(filename="EACDataExport_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv")
    #ViralLoadAndIITPattern.export_iit_vl_data(filename="ViralLoadAndIITPattern_Output_"+datetime.now().strftime("_%Y_%m_%d %H_%M_%S")+".csv")
    EACDataExportMultiprocess.producer_consumer_etl(
            num_consumers=10,
            filename="EACDataExportMultiprocess_Output_"+datetime.now().strftime("%Y_%m_%d_%H_%M_%S")+".csv"
        )

    end_time = datetime.now()
    print ("End time: ", end_time )
    duration = end_time - start_time
    print ("Duration: ", duration )
