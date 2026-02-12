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



def export_tb_outcome_study_data(cutoff_datetime=None, filename=None ):
    db_name="cdr"
    asokoro_datim_code = "KFbRZKvXpb3"
    national_hospital_datim_code = "GW1w1chZMPR"
    datim_codes = [asokoro_datim_code, national_hospital_datim_code]
    db = mongo_dao.get_db_connection(db_name)
    cursor = mongo_dao.get_containers_by_datim_list(db, datim_codes, db_name)
    size = mongo_dao.get_container_by_datim_list_size(db, datim_codes, db_name)
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 1000
    batch_list = []

    # 1. Prepare the file path (create directory and name)
    full_path = prepare_filepath(filename)
    
    # Track if it's the first batch so we can write the CSV header
    is_first_batch = True
    
    #extracted_results = []
    for doc in tqdm(cursor, total=size, desc="TB Outcome Study ETL Progress"):
            
           
            if not is_aspire_state(doc):
                continue  # Skip this record and move to the next one

            header = demographicsutils.get_message_header(doc)
            datim_code = header.get("facilityDatimCode")
            demographics = demographicsutils.get_patient_demographics(doc)
            birthdate = commonutils.validate_date(demographics.get("birthdate"))
            facility_info = get_facility_by_datim(datim_code)
            art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
            
           
            current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime) 
            current_viral_load_obsdatetime = obsutils.getObsDatetimeFromObs(current_viral_load_obs) if current_viral_load_obs else None 
            last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime) 
            baseline_weight = carecardutils.get_first_weight_obs(doc,cutoff_datetime)
            baseline_weight_value = obsutils.getValueNumericFromObs(baseline_weight) if baseline_weight else None
            baseline_weight_date = obsutils.getObsDatetimeFromObs(baseline_weight) if baseline_weight else None
            baseline_viral_load_obs = labutils.get_first_viral_load_obs(doc)
            baseline_viral_load_value = obsutils.getValueNumericFromObs(baseline_viral_load_obs) if baseline_viral_load_obs else None
            baseline_viral_load_date = obsutils.getObsDatetimeFromObs(baseline_viral_load_obs) if baseline_viral_load_obs else None
            baseline_who_stage_obs = carecardutils.get_first_who_stage_obs(doc,cutoff_datetime)
            baseline_who_stage_value = obsutils.getVariableValueFromObs(baseline_who_stage_obs) if baseline_who_stage_obs else None
            baseline_who_stage_date = obsutils.getObsDatetimeFromObs(baseline_who_stage_obs) if baseline_who_stage_obs else None
            baseline_tb_status_obs = carecardutils.get_first_tb_status_obs(doc,cutoff_datetime)
            baseline_tb_status_value = obsutils.getVariableValueFromObs(baseline_tb_status_obs) if baseline_tb_status_obs else None
            baseline_tb_status_date = obsutils.getObsDatetimeFromObs(baseline_tb_status_obs) if baseline_tb_status_obs else None
            inh_pickup1_date_obs = pharmacyutils.get_nth_pickup_isoniazid_prophylaxis_obs_of_last_x_pickups(doc, 1, 5)
            inh_pickup1_date = obsutils.getObsDatetimeFromObs(inh_pickup1_date_obs) if inh_pickup1_date_obs else None
            inh_pickup2_date_obs = pharmacyutils.get_nth_pickup_isoniazid_prophylaxis_obs_of_last_x_pickups(doc, 2, 5)
            inh_pickup2_date = obsutils.getObsDatetimeFromObs(inh_pickup2_date_obs) if inh_pickup2_date_obs else None
            inh_pickup3_date_obs = pharmacyutils.get_nth_pickup_isoniazid_prophylaxis_obs_of_last_x_pickups(doc, 3, 5)
            inh_pickup3_date = obsutils.getObsDatetimeFromObs(inh_pickup3_date_obs) if inh_pickup3_date_obs else None
            inh_pickup4_date_obs = pharmacyutils.get_nth_pickup_isoniazid_prophylaxis_obs_of_last_x_pickups(doc, 4, 5)
            inh_pickup4_date = obsutils.getObsDatetimeFromObs(inh_pickup4_date_obs) if inh_pickup4_date_obs else None
            inh_pickup5_date_obs = pharmacyutils.get_nth_pickup_isoniazid_prophylaxis_obs_of_last_x_pickups(doc, 5, 5)
            inh_pickup5_date = obsutils.getObsDatetimeFromObs(inh_pickup5_date_obs) if inh_pickup5_date_obs else None
            tb_status1_obs = carecardutils.get_last_nth_tb_status_obs_of_last_x_tb_statuses(doc, 1, 5, cutoff_datetime)
            tb_status1_value = obsutils.getVariableValueFromObs(tb_status1_obs) if tb_status1_obs else None
            tb_status1_date = obsutils.getObsDatetimeFromObs(tb_status1_obs) if tb_status1_obs else None
            tb_status2_obs = carecardutils.get_last_nth_tb_status_obs_of_last_x_tb_statuses(doc, 2, 5, cutoff_datetime)
            tb_status2_value = obsutils.getVariableValueFromObs(tb_status2_obs) if tb_status2_obs else None
            tb_status2_date = obsutils.getObsDatetimeFromObs(tb_status2_obs) if tb_status2_obs else None
            tb_status3_obs = carecardutils.get_last_nth_tb_status_obs_of_last_x_tb_statuses(doc, 3, 5, cutoff_datetime)
            tb_status3_value = obsutils.getVariableValueFromObs(tb_status3_obs) if tb_status3_obs else None
            tb_status3_date = obsutils.getObsDatetimeFromObs(tb_status3_obs) if tb_status3_obs else None
            tb_status4_obs = carecardutils.get_last_nth_tb_status_obs_of_last_x_tb_statuses(doc, 4, 5, cutoff_datetime)
            tb_status4_value = obsutils.getVariableValueFromObs(tb_status4_obs) if tb_status4_obs else None
            tb_status4_date = obsutils.getObsDatetimeFromObs(tb_status4_obs) if tb_status4_obs else None
            tb_status5_obs = carecardutils.get_last_nth_tb_status_obs_of_last_x_tb_statuses(doc, 5, 5, cutoff_datetime)
            tb_status5_value = obsutils.getVariableValueFromObs(tb_status5_obs) if tb_status5_obs else None
            tb_status5_date = obsutils.getObsDatetimeFromObs(tb_status5_obs) if tb_status5_obs else None   
            last_tb_diagnosed_obs = carecardutils.get_last_tb_diagnosed_obs(doc, cutoff_datetime)
            last_tb_diagnosed_date = obsutils.getObsDatetimeFromObs(last_tb_diagnosed_obs) if last_tb_diagnosed_obs else None 
            last_inh_pickup_obs = pharmacyutils.get_last_isoniazid_prophylaxis_pickup_obs(doc, cutoff_datetime)
            last_inh_pickup_date = obsutils.getObsDatetimeFromObs(last_inh_pickup_obs) if last_inh_pickup_obs else None
            last_tb_status_obs = carecardutils.get_current_tb_status_obs(doc, cutoff_datetime)
            last_tb_status_value = obsutils.getVariableValueFromObs(last_tb_status_obs) if last_tb_status_obs else None
            last_tb_status_date = obsutils.getObsDatetimeFromObs(last_tb_status_obs) if last_tb_status_obs else None
            last_who_stage_obs = carecardutils.get_last_who_stage_obs(doc, cutoff_datetime)
            last_who_stage_value = obsutils.getVariableValueFromObs(last_who_stage_obs) if last_who_stage_obs else None
            last_who_stage_date = obsutils.getObsDatetimeFromObs(last_who_stage_obs) if last_who_stage_obs else None

            record = {
                "touchtime": header.get("touchTime"),
                "State": facility_info.get("State") if facility_info else None,
                "LGA" : facility_info.get("LGA") if facility_info else None,
                "DatimCode" : header.get("facilityDatimCode"),
                "FacilityName": header.get("facilityName"),
                "UniqueID": demographicsutils.get_patient_identifier(4, doc),
                "HospitalNumber": demographicsutils.get_patient_identifier(5, doc),
                "Sex": demographics.get("gender"),
                "CurrentAgeYears": demographicsutils.get_current_age_at_date(doc,cutoff_datetime),
                "AgeAtARTStartYears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
                "DOB": birthdate,
                "CareEntryPoint": hivenrollmentutils.get_care_entry_point(doc,cutoff_datetime),
                "MonthsOnArt": demographicsutils.get_months_on_art(doc,art_start_date,cutoff_datetime),
                "DateTransferredIn": hivenrollmentutils.get_date_transferred_in(doc,cutoff_datetime),
                "TransferredInStatus": hivenrollmentutils.get_prior_art(doc,cutoff_datetime),
                "ArtStartDate": art_start_date,
                "BaselineWeight(Kg)": baseline_weight_value,
                "BaselineWeightDate": baseline_weight_date,
                "BaselineViralLoad": baseline_viral_load_value,
                "BaselineViralLoadDate": baseline_viral_load_date,
                "BaselineWHOStage": baseline_who_stage_value,
                "BaselineWHOStageDate": baseline_who_stage_date,
                "BaselineTBStatus": baseline_tb_status_value,
                "BaselineTBStatusDate": baseline_tb_status_date,
                "INHPickup1Date": inh_pickup1_date,
                "INHPickup2Date": inh_pickup2_date,
                "INHPickup3Date": inh_pickup3_date,
                "INHPickup4Date": inh_pickup4_date,
                "INHPickup5Date": inh_pickup5_date,
                "TBStatus1": tb_status1_value,
                "TBStatus1Date": tb_status1_date,   
                "TBStatus2": tb_status2_value,
                "TBStatus2Date": tb_status2_date,   
                "TBStatus3": tb_status3_value,
                "TBStatus3Date": tb_status3_date,   
                "TBStatus4": tb_status4_value,
                "TBStatus4Date": tb_status4_date,   
                "TBStatus5": tb_status5_value,
                "TBStatus5Date": tb_status5_date,   
                "SecondLineRegimenStartDate": pharmacyutils.get_min_second_line_regimen_date(doc,cutoff_datetime),
                "ThirdLineRegimenStartDate": pharmacyutils.get_min_third_line_regimen_date(doc,cutoff_datetime),
                "LastPickupDate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
                "DaysOfARVRefill": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
                "PillBalance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),
                "PatientOutcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime),
                "PatientOutcomeDate" : ctdutils.get_outcome_date (doc,cutoff_datetime),
                "LastTBDiagnosedDate": last_tb_diagnosed_date,
                "CurrentArtStatus": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
                "CurrentRegimenLine": pharmacyutils.get_current_regimen_line(doc,cutoff_datetime) ,
                "CurrentRegimen": pharmacyutils.get_current_regimen(doc,cutoff_datetime),
                "CurrentINHDate":  last_inh_pickup_date,
                "CurrentTBStatus": last_tb_status_value,
                "CurrentTBStatusDate": last_tb_status_date,
                "CurrentWHOStage": last_who_stage_value,
                "CurrentWHOStageDate": last_who_stage_date,
                "CurrentViralLoad": obsutils.getValueNumericFromObs(current_viral_load_obs),
                "CurrentViralLoadDate": current_viral_load_obsdatetime,
                "PatientUUID": demographicsutils.get_patient_demographics(doc).get("patientUuid")  
                
                
                
                
                
                
               
                
                
                
                      
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




