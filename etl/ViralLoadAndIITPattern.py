#import mongo_utils as  utils
import dao.mongodbdao as mongo_dao
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import os

import dao.mongodbdao as mongo_dao
import utils.demographicutils as demographicsutils
import formslib.artcommencementutil as artcommence
import formslib.hivenrollmentutil as hivenrollmentutils
import formslib.carecardutils as carecardutils
import formslib.pharmacyutils as pharmacyutils
import utils.encounterutils as encounterutils
import formslib.labutils as labutils
import formslib.eacutils as eacutils
import utils.obsutils as obsutils
import formslib.ctdutils as ctdutils
import utils.commonutils as commonutils

def export_iit_vl_data(cutoff_datetime=None, filename=None ):
    db = mongo_dao.get_db_connection("cdr")
    cursor = mongo_dao.get_art_containers(db)
    size = mongo_dao.get_art_container_size(db)
    print(f"Processing {size} ART containers...")

    # Define batch settings
    BATCH_SIZE = 1000 
    batch_list = []
    
    # 1. Prepare the file path (create directory and name)
    full_path = prepare_filepath(filename)
    
    # Track if it's the first batch so we can write the CSV header
    is_first_batch = True
    
    for doc in tqdm(cursor, total=size, desc="ViralLoadAndIITPatternETL Progress"):
            header =demographicsutils.get_message_header(doc)
            demographics = demographicsutils.get_patient_demographics(doc)
            birthdate = commonutils.validate_date(demographics.get("birthdate"))
            art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
            pickup_1_date_obs = pharmacyutils.get_nth_pickup_obs_of_last_x_pickups(doc, 1, 5, cutoff_datetime)
            pickup_2_date_obs = pharmacyutils.get_nth_pickup_obs_of_last_x_pickups(doc, 2, 5, cutoff_datetime)
            pickup_3_date_obs = pharmacyutils.get_nth_pickup_obs_of_last_x_pickups(doc, 3, 5, cutoff_datetime)
            pickup_4_date_obs = pharmacyutils.get_nth_pickup_obs_of_last_x_pickups(doc, 4, 5, cutoff_datetime)
            pickup_5_date_obs = pharmacyutils.get_nth_pickup_obs_of_last_x_pickups(doc, 5, 5, cutoff_datetime)
            pickup_1_date= obsutils.getObsDatetimeFromObs(pickup_1_date_obs) 
            pickup_2_date= obsutils.getObsDatetimeFromObs(pickup_2_date_obs) 
            pickup_3_date= obsutils.getObsDatetimeFromObs(pickup_3_date_obs) 
            pickup_4_date= obsutils.getObsDatetimeFromObs(pickup_4_date_obs) 
            pickup_5_date= obsutils.getObsDatetimeFromObs(pickup_5_date_obs) 
            viral_load1_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, 1, 5, cutoff_datetime)
            viral_load2_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, 2, 5, cutoff_datetime)
            viral_load3_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, 3, 5, cutoff_datetime)
            viral_load4_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, 4, 5, cutoff_datetime)
            viral_load5_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, 5, 5, cutoff_datetime)

            viral_load1_date= obsutils.getObsDatetimeFromObs(viral_load1_obs)
            viral_load2_date= obsutils.getObsDatetimeFromObs(viral_load2_obs)
            viral_load3_date= obsutils.getObsDatetimeFromObs(viral_load3_obs)
            viral_load4_date= obsutils.getObsDatetimeFromObs(viral_load4_obs)
            viral_load5_date= obsutils.getObsDatetimeFromObs(viral_load5_obs)
            viral_load_value1= obsutils.getValueNumericFromObs(viral_load1_obs) 
            viral_load_value2= obsutils.getValueNumericFromObs(viral_load2_obs)
            viral_load_value3= obsutils.getValueNumericFromObs(viral_load3_obs)
            viral_load_value4= obsutils.getValueNumericFromObs(viral_load4_obs)
            viral_load_value5= obsutils.getValueNumericFromObs(viral_load5_obs)

            current_viral_load_obs = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, 1, 1, cutoff_datetime)



            
            record = {
                "touchtime": header.get("touchTime"),
                "facilityState": header.get("facilityState"),
                "facilityLga" : header.get("facilityLga"),
                "facilityDatimCode" : header.get("facilityDatimCode"),
                "facilityName": header.get("facilityName"),
                "patientUniqueID": demographicsutils.get_patient_identifier(4, doc),
                "patientHospitalID": demographicsutils.get_patient_identifier(5, doc),
                
                "sex": demographics.get("gender"),
                "birthdate": birthdate,
            
                "artStartDate": art_start_date,
                "ageAtARTStartMonths": demographicsutils.get_pediatric_age_art_start_months(doc,birthdate,art_start_date),
                "ageAtARTStartYears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
                "careEntryPoint": hivenrollmentutils.get_care_entry_point(doc,cutoff_datetime),
                "careEntryPoint": hivenrollmentutils.get_care_entry_point(doc,cutoff_datetime),
                "monthsOnArt": demographicsutils.get_months_on_art(doc,art_start_date,cutoff_datetime),
                "pickup_1_date": pickup_1_date,
                "pickup_2_date": pickup_2_date, 
                "pickup_3_date": pickup_3_date,
                "pickup_4_date": pickup_4_date,     
                "pickup_5_date": pickup_5_date,
                "viral_load_1_date": viral_load1_date,  
                "viral_load_1_value": viral_load_value1,
                "viral_load_2_date": viral_load2_date,  
                "viral_load_2_value": viral_load_value2,
                "viral_load_3_date": viral_load3_date,  
                "viral_load_3_value": viral_load_value3,
                "viral_load_4_date": viral_load4_date,  
                "viral_load_4_value": viral_load_value4,
                "viral_load_5_date": viral_load5_date,  
                "viral_load_5_value": viral_load_value5,

                "currentRegimenLine": pharmacyutils.get_current_regimen_line(doc,cutoff_datetime) ,
                "currentRegimen": pharmacyutils.get_current_regimen(doc,cutoff_datetime),
                "secondLineRegimenStartDate": pharmacyutils.get_min_second_line_regimen_date(doc,cutoff_datetime),
                "thirdLineRegimenStartDate": pharmacyutils.get_min_third_line_regimen_date(doc,cutoff_datetime),
                "lastPickupDate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
                "daysOfARVRefill": pharmacyutils.get_last_drug_pickup_duration(doc,cutoff_datetime),                
                "currentViralLoad": obsutils.getValueNumericFromObs(current_viral_load_obs),
                "currentViralLoadDate": obsutils.getObsDatetimeFromObs(current_viral_load_obs),
                "patientOutcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime),
                "patientOutcomeDate" : ctdutils.get_outcome_date (doc,cutoff_datetime),
                "currentArtStatus": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
                "currentAge": demographicsutils.get_current_age_at_date(doc,cutoff_datetime),
                "currentAgeInMonths": demographicsutils.get_current_age_at_date_in_months(doc,cutoff_datetime),
                "patientUUID": demographicsutils.get_patient_demographics(doc).get("patientUuid"),
               
                
            }

            batch_list.append(record)

        # 2. Check if batch is full
            if len(batch_list) >= BATCH_SIZE:
                 save_batch_to_csv(batch_list, full_path, is_first_batch)
                 batch_list = [] # Clear memory
                 is_first_batch = False # Next batches append without headers

        # 3. Save any remaining records (the last partial batch)
    if batch_list:
        save_batch_to_csv(batch_list, full_path, is_first_batch)

    print(f"\nFinal export complete. Total records processed: {size}")
    print(f"File saved to: {full_path}")
    return full_path




def prepare_filepath(filename=None):
    """Creates the directory and generates the full path for the CSV."""
    output_dir = './output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if filename:
        if not filename.endswith('.csv'):
            filename = f"{filename}_{timestamp}.csv"
    else:
        filename = f"EACExport_{timestamp}.csv"
        
    return os.path.join(output_dir, filename)


def save_batch_to_csv(batch_data, full_path, write_header):
    """Writes a single batch of data to the CSV file."""
    df = pd.DataFrame(batch_data)
    # mode='a' means Append
    # header=write_header ensures the column names only appear at the top
    df.to_csv(full_path, mode='a', index=False, header=write_header)




