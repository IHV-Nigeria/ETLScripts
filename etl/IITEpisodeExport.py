#import mongo_utils as  utils
#import constants as constants
from email import utils
import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
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



# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}



def export_iit_episode_data(filename=None ):
    db_name="cdr"
    cutoff_datetime = datetime(2025, 12, 31, 23, 59, 59)
    db = mongo_dao.get_db_connection(db_name)
    cursor = mongo_dao.get_art_containers(db,db_name)
    size = mongo_dao.get_art_container_size(db,db_name)
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 1000
    batch_list = []

    # 1. Prepare the file path (create directory and name)
    full_path = prepare_filepath(filename)
    
    # Track if it's the first batch so we can write the CSV header
    is_first_batch = True
    
    #extracted_results = []
    for doc in tqdm(cursor, total=size, desc="IIT Episode ETL Progress"):
            
           
            if not is_aspire_state(doc):
                continue  # Skip this record and move to the next one

            header = demographicsutils.get_message_header(doc)
            datim_code = header.get("facilityDatimCode")
            demographics = demographicsutils.get_patient_demographics(doc)
            birthdate = commonutils.validate_date(demographics.get("birthdate"))
            facility_info = get_facility_by_datim(datim_code)
            art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc))
            eac_1_date = commonutils.validate_date(eacutils.get_eac_date(1, doc))
            last_eac_encounter=eacutils.get_last_eac_encounter(doc,cutoff_datetime)
            current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime) 
            current_viral_load_obsdatetime = obsutils.getObsDatetimeFromObs(current_viral_load_obs) if current_viral_load_obs else None 
            last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime) 
            current_pregnancy_status_obs=carecardutils.get_current_pregnancy_status_obs(doc,cutoff_datetime)

            last_marital_status_obs = hivenrollmentutils.get_last_marital_status_obs(doc,cutoff_datetime)
            last_marital_status_value = obsutils.getVariableValueFromObs(last_marital_status_obs) if last_marital_status_obs else None
            last_occupational_status_obs = hivenrollmentutils.get_last_occupational_status_obs(doc,cutoff_datetime)
            last_occupational_status_value = obsutils.getVariableValueFromObs(last_occupational_status_obs) if last_occupational_status_obs else None
            last_education_level_obs = hivenrollmentutils.get_last_education_level_obs(doc,cutoff_datetime)
            last_education_level_value = obsutils.getVariableValueFromObs(last_education_level_obs) if last_education_level_obs else None
            last_date_confirmed_hiv_positive_obs = hivenrollmentutils.get_last_date_confirmed_hiv_positive_obs(doc,cutoff_datetime)
            last_date_confirmed_hiv_positive_value = obsutils.getValueDatetimeFromObs(last_date_confirmed_hiv_positive_obs) if last_date_confirmed_hiv_positive_obs else None
            baseline_viral_load_obs = labutils.get_first_viral_load_obs(doc) 
            baseline_tb_status_obs = carecardutils.get_first_tb_status_obs(doc)


            record = {
                "touchtime": header.get("touchTime"),
                "state": facility_info.get("State") if facility_info else None,
                "lga" : facility_info.get("LGA") if facility_info else None,
                "datim_code" : header.get("facilityDatimCode"),
                "facility_name": header.get("facilityName"),
                "patient_unique_id": demographicsutils.get_patient_identifier(4, doc),
                "patient_hospital_number": demographicsutils.get_patient_identifier(5, doc),
                "sex": demographics.get("gender"),
                "date_of_birth": birthdate,
                "current_age_years": demographicsutils.get_current_age_at_date(doc),
                "current_age_months": demographicsutils.get_current_age_at_date_in_months(doc),
                "age_at_art_start_years": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
                "age_at_art_start_months": demographicsutils.get_pediatric_age_art_start_months(doc, birthdate, art_start_date),
                "occupational_status": last_occupational_status_value,
                "marital_status": last_marital_status_value,
                "education_level": last_education_level_value,
                "care_entry_point": hivenrollmentutils.get_care_entry_point(doc),
                "date_confirmed_hiv_positive": last_date_confirmed_hiv_positive_value,
                "art_start_date": art_start_date,
                "date_transferred_in": hivenrollmentutils.get_date_transferred_in(doc),
                "transferred_in_status": hivenrollmentutils.get_prior_art(doc), 
                "time_from_diagnosis_to_art_start_days": commonutils.get_days_diff(last_date_confirmed_hiv_positive_value, art_start_date) , 
                "total_time_on_art(days)": commonutils.get_days_diff(art_start_date,cutoff_datetime),
                "baseline_viral_load": obsutils.getValueNumericFromObs(baseline_viral_load_obs),
                "baseline_viral_load_date": obsutils.getObsDatetimeFromObs(baseline_viral_load_obs),
                "baseline_tb_status": obsutils.getVariableValueFromObs(baseline_tb_status_obs),
                "regimen_line_at_art_start": artcommence.get_current_regimen_line(doc),
                "regimen_at_art_start": artcommence.get_current_regimen(doc),
                "who_stage_at_art_start": artcommence.get_who_stage_at_art_start(doc),
                "cd4_count_at_art_start": artcommence.get_cd4_count_at_art_start(doc),
                "pregnancy_status_at_art_start": artcommence.get_pregnancy_status_at_art_start(doc),
                "last_pickup_date": pharmacyutils.get_last_arv_pickup_date(doc),
                "last_pickup_duration_days": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
                "current_art_status": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
                "patient_outcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime), 
                "patient_outcome_date" : ctdutils.get_outcome_date (doc,cutoff_datetime),
                "current_pregnancy_status": obsutils.getVariableValueFromObs(current_pregnancy_status_obs) if current_pregnancy_status_obs else None,
                "current_pregnancy_status_date": obsutils.getObsDatetimeFromObs(current_pregnancy_status_obs) if current_pregnancy_status_obs else None,
                "current_viral_load": obsutils.getValueNumericFromObs(current_viral_load_obs),
                "current_viral_load_date": current_viral_load_obsdatetime,

                
                        
                          
                           
                
                
                
              
                
                
                
                      
            }
            batch_list.append(record)

            if len(batch_list) >= BATCH_SIZE:
                save_batch_to_csv(batch_list, full_path, is_first_batch)
                batch_list = [] # Clear memory
                is_first_batch = False # Next batches append without headers

    
    # 3. Save any remaining records (the last partial batch)
    if batch_list:
        save_batch_to_csv(batch_list, full_path, is_first_batch)
                 
    #df = pd.DataFrame(extracted_results)
    #print(f"Found {len(df)} matching records.")
    #print(df.head(20))
    #return export_dataframe(df, filename)
    db.client.close()
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

def load_facility_cache(db, db_name="cdr"):
    """
    Loads all facilities into a dictionary indexed by DATIM code.
    Run this once at the start of your ETL.
    """
    global _facility_cache
    facilities = mongo_dao.get_all_facilities(db, db_name)
    # Create a dictionary: { "DATIM_CODE": {full_json_metadata} }
    _facility_cache = {f.get("DATIM"): f for f in facilities if f.get("DATIM")}
    print(f"Loaded {len(_facility_cache)} facilities into memory cache.")

def get_facility_by_datim(datim_code):
    """
    Returns the full facility JSON for a given DATIM code.
    Returns None if the code is not found.
    """
    return _facility_cache.get(datim_code)

# check if document belongs to a facility in ASPIRE states (FCT,Katsina,Nasarawa,Rivers) ignore casing and whitespace
def is_aspire_state(doc):
    aspire_states = ["FCT", "KATSINA", "NASARAWA", "RIVERS"]
    header = demographicsutils.get_message_header(doc)
    datim_code = header.get("facilityDatimCode")
   
    if not datim_code:
        return False    
    facility = get_facility_by_datim(datim_code)
    
    if facility is None:
        return False
    if facility:
        state = facility.get("State", "").strip().upper()
        return state in aspire_states
    return False




