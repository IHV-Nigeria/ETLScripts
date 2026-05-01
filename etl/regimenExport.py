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
from dao.config import MONGO_DATABASE_NAME
from legacy.constants import PHARMACY_FORM_ID
from utils.obsutils import get_last_obs_before_date

# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}



def export_regimen_data(cutoff_datetime=None, filename=None):
    db_name=MONGO_DATABASE_NAME
    db = mongo_dao.get_db_connection(db_name)

    cursor = mongo_dao.get_art_containers(db, db_name)
    size=725000

    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 1000
    batch_list = []

    cutoff_datetime = commonutils.normalize_clinical_date(datetime(2024, 10, 1)) if cutoff_datetime else None



    # 1. Prepare the file path (create directory and name)
    full_path = prepare_filepath(filename)

    # Track if it's the first batch so we can write the CSV header
    is_first_batch = True

    #extracted_results = []
    for doc in tqdm(cursor, total=size, desc="EAC ETL Progress"):


        if not is_aspire_state(doc):
            continue  # Skip this record and move to the next one

        header = demographicsutils.get_message_header(doc)
        datim_code = header.get("facilityDatimCode")
        demographics = demographicsutils.get_patient_demographics(doc)
        birthdate = commonutils.normalize_clinical_date(demographics.get("birthdate"))
        facility_info = get_facility_by_datim(datim_code)
        art_start_date = commonutils.normalize_clinical_date(artcommence.get_art_start_date(doc, cutoff_datetime))

        last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)
        initial_regimen_line_obs = pharmacyutils.get_first_regimen_line(doc)
        initial_regimen_line = initial_regimen_line_obs.get("variableValue") if initial_regimen_line_obs else None
        initial_regimen_line_date = initial_regimen_line_obs.get("obsDatetime") if initial_regimen_line_obs else None

        second_line_regimen_obs = pharmacyutils.get_min_second_line_regimen_obs(doc,cutoff_datetime)
        second_line_regimen = second_line_regimen_obs.get("variableValue") if second_line_regimen_obs else None

        third_line_regimen_obs = pharmacyutils.get_min_third_line_regimen_obs(doc,cutoff_datetime)
        third_line_regimen = third_line_regimen_obs.get("variableValue") if third_line_regimen_obs else None

        previous_regimen_obs = pharmacyutils.get_previous_regimen_last_obs(doc)
        previous_regimen_line = previous_regimen_obs.get("variableName") if previous_regimen_obs else None
        previous_regimen = (previous_regimen_obs.get("variableValue") if previous_regimen_obs else None)
        previous_regimen_date = (previous_regimen_obs.get("obsDatetime") if previous_regimen_obs else None)

        record = {
            "touchtime": header.get("touchTime"),
            "State": facility_info.get("State") if facility_info else None,
            "LGA" : facility_info.get("LGA") if facility_info else None,
            "DatimCode" : header.get("facilityDatimCode"),
            "FacilityName": header.get("facilityName"),
            "UniqueID": demographicsutils.get_patient_identifier(4, doc),
            "HospitalNumber": demographicsutils.get_patient_identifier(5, doc),
            "Sex": demographics.get("gender"),
            "AgeAtARTStartYears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
            "AgeAtARTStartMonths": demographicsutils.get_pediatric_age_art_start_months(doc, birthdate, art_start_date),
            "CurrentAgeYears": demographicsutils.get_current_age_at_date(doc,cutoff_datetime),
            "CurrentAgeMonths": demographicsutils.get_current_age_at_date_in_months(doc,cutoff_datetime),
            "DOB": birthdate,
            "CareEntryPoint": hivenrollmentutils.get_care_entry_point(doc,cutoff_datetime),
            "MonthsOnArt": demographicsutils.get_months_on_art(doc,art_start_date,cutoff_datetime),
            "DateTransferredIn": hivenrollmentutils.get_date_transferred_in(doc,cutoff_datetime),
            "TransferredInStatus": hivenrollmentutils.get_prior_art(doc,cutoff_datetime),
            "ArtStartDate": art_start_date,

            "LastVisitDate": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
            "LastPickupDate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
            "DaysOfARVRefill": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
            "PillBalance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),
            "PatientOutcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime),
            "PatientOutcomeDate" : ctdutils.get_outcome_date (doc,cutoff_datetime),

            # "PharmacyNextAppointmentDate": pharmacyutils.get_pharmacy_next_appointment_date(doc, cutoff_datetime),
            # "ClinicalNextAppointmentDate": carecardutils.get_clinical_next_appointment_date(doc,cutoff_datetime),

            "InitialRegimenLine": initial_regimen_line,
            "InitialRegimenLineDate": initial_regimen_line_date,
            "InitialRegimen": pharmacyutils.get_initial_regimen(doc),

            "PreviousRegimenLine": previous_regimen_line,
            "PreviousRegimen": previous_regimen,
            "PreviousRegimenLastDate": previous_regimen_date,

            "CurrentRegimenLine": pharmacyutils.get_current_regimen_line(doc,cutoff_datetime),
            "CurrentRegimen": pharmacyutils.get_current_regimen(doc,cutoff_datetime),
            "CurrentRegimenStartDate": pharmacyutils.get_current_regimen_line_start_date(doc),

            "SecondLineRegimenStartDate": pharmacyutils.get_min_second_line_regimen_date(doc,cutoff_datetime),
            "SecondLineRegimen": second_line_regimen,

            "ThirdLineRegimenStartDate": pharmacyutils.get_min_third_line_regimen_date(doc,cutoff_datetime),
            "ThirdLineRegimen": third_line_regimen,

        }
        print("Previous Regimen: ", previous_regimen_line, "Current Regimen: ",  record.get("CurrentRegimenLine"))
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




