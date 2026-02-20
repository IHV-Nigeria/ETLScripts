#import mongo_utils as utils
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
from dao import config

# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}



def export_cdr_line_list_data(cutoff_datetime=None, filename=None ):
    """
    Main function to export CDR Line List data to CSV. Processes ART containers in batches and saves to CSV incrementally.
    target variables to extract:
    State, LGA, DatimCode, FacilityName, UniqueID, HospitalNumber, Sex, DateOfBirth,LastVisitDate,
    LastARTPickupDate, DurationOfLastARVPickup, PatientOutcome, PatientOutcomeDate, CurrentARTStatus
    """
    db_name=config.MONGO_DATABASE_NAME
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
    for doc in tqdm(cursor, total=size, desc="CDR Line List ETL Progress"):
            
           
            if not is_aspire_state(doc):
                continue  # Skip this record and move to the next one

            header = demographicsutils.get_message_header(doc)
            datim_code = header.get("facilityDatimCode")
            demographics = demographicsutils.get_patient_demographics(doc)
            birthdate = commonutils.validate_date(demographics.get("birthdate"))
            facility_info = get_facility_by_datim(datim_code)
            art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
            last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime) 
           


            record = {
                "touchtime": header.get("touchTime"),
                "State": facility_info.get("State") if facility_info else None,
                "LGA" : facility_info.get("LGA") if facility_info else None,
                "DatimCode" : header.get("facilityDatimCode"),
                "FacilityName": header.get("facilityName"),
                "UniqueID": demographicsutils.get_patient_identifier(4, doc),
                "HospitalNumber": demographicsutils.get_patient_identifier(5, doc),
                "Sex": demographics.get("gender"),
                "DOB": birthdate,
                "ArtStartDate": art_start_date,
                "LastPickupDate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
                "LastVisitDate": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
                "DaysOfARVRefill": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
                "PillBalance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),
                "PatientOutcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime),
                "PatientOutcomeDate" : ctdutils.get_outcome_date (doc,cutoff_datetime),
                "CurrentArtStatus": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
                               
            }
            batch_list.append(record)

            if len(batch_list) >= BATCH_SIZE:
                save_batch_to_csv(batch_list, full_path, is_first_batch)
                batch_list = [] # Clear memory
                is_first_batch = False # Next batches append without headers

    
    # 3. Save any remaining records (the last partial batch)
    if batch_list:
        save_batch_to_csv(batch_list, full_path, is_first_batch)
                 
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

def load_facility_cache(db, db_name=config.MONGO_DATABASE_NAME):
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




