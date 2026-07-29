import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
from collections import OrderedDict
import os
import logging
import json
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
import dao.postgresdao as postgres_dao
from dao.config import MONGO_DATABASE_NAME

# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}


def export_data(cutoff_datetime=None, start_date=None, end_date=None, filename=None):
    db_name = MONGO_DATABASE_NAME
    db = mongo_dao.get_db_connection(db_name)
    cursor, size = mongo_dao.get_art_containers(db=db)

    # Normalize dates if provided
    if start_date:
        start_date = commonutils.normalize_clinical_date(start_date)
    if end_date:
        end_date = commonutils.normalize_clinical_date(end_date)
    if cutoff_datetime:
        cutoff_datetime = commonutils.normalize_clinical_date(cutoff_datetime)

    print(f"Processing {size} ART containers for Viral Load extraction...")
    if start_date or end_date:
        print(f"Date filter: {start_date} to {end_date}")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 1000
    batch_list = []

    # 1. Prepare the file path (create directory and name)
    full_path = prepare_filepath(filename)

    # Track if it's the first batch so we can write the CSV header
    is_first_batch = True

    for doc in tqdm(cursor, total=size, desc="Viral Load Extraction ETL Progress"):
        header = demographicsutils.get_message_header(doc)
        datim_code = header.get("facilityDatimCode")
        demographics = demographicsutils.get_patient_demographics(doc)
        birthdate = commonutils.normalize_clinical_date(demographics.get("birthdate"))
        facility_info = get_facility_by_datim(datim_code)
        art_start_date = commonutils.normalize_clinical_date(artcommence.get_art_start_date(doc, cutoff_datetime))

        # Get pharmacy data (drug pickup and regimen)
        last_arv_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)
        last_drug_pickup_date = obsutils.getObsDatetimeFromObs(last_arv_obs) if last_arv_obs else None
        last_regimen = pharmacyutils.get_current_regimen(doc, cutoff_datetime)
        days_of_arv_refill = pharmacyutils.get_last_drug_pickup_duration(doc, last_arv_obs)

        # Get all viral load observations (non-voided obs from LAB form with VIRAL_LOAD concept)
        viral_load_obs_list = get_all_viral_load_obs(doc, cutoff_datetime)

        # Filter by date range if provided
        if start_date or end_date:
            viral_load_obs_list = [vl for vl in viral_load_obs_list 
                                   if is_obs_within_date_range(vl, start_date, end_date)]

        # If no viral loads after filtering, skip this patient
        if not viral_load_obs_list:
            continue

        # Create one record per viral load observation
        for vl_obs in viral_load_obs_list:
            viral_load = obsutils.getValueNumericFromObs(vl_obs)
            
            # Use sample collection date as the primary date for viral load entry
            viral_load_sample_collection_date = obsutils.getValueDatetimeFromObs(
                labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, vl_obs))
            
            viral_load_report_date = obsutils.getValueDatetimeFromObs(
                labutils.get_reported_date_obs_of_viral_load_obs(doc, vl_obs))
            
            indication_for_viral_load_test = obsutils.getVariableValueFromObs(
                labutils.get_viral_load_indication_obs_of_viral_load_obs(doc, vl_obs))

            # Visit date is the sample collection date if available, otherwise observation date
            visit_date = viral_load_sample_collection_date or obsutils.getObsDatetimeFromObs(vl_obs)

            # Get pregnancy status for this specific visit/encounter
            pregnancy_status = get_pregnancy_status_for_encounter(doc, vl_obs.get('encounterId'))

            # Get status (e.g., suppressed or not)
            status = "Suppressed" if viral_load and viral_load < 1000 else "Unsuppressed" if viral_load else None

            record = create_base_record(
                doc, header, demographics, birthdate, facility_info, datim_code,
                art_start_date, pregnancy_status, visit_date,
                last_drug_pickup_date, last_regimen, days_of_arv_refill,
                indication_for_viral_load_test, viral_load,
                viral_load_sample_collection_date, viral_load_report_date,
                status, visit_date
            )
            batch_list.append(record)

        if len(batch_list) >= BATCH_SIZE:
            save_batch_to_csv(batch_list, full_path, is_first_batch)
            batch_list = []  # Clear memory
            is_first_batch = False  # Next batches append without headers

    # 3. Save any remaining records (the last partial batch)
    if batch_list:
        save_batch_to_csv(batch_list, full_path, is_first_batch)

    db.client.close()
    print(f"\nFinal export complete. Total records processed: {size}")
    print(f"File saved to: {full_path}")
    return full_path


def create_base_record(doc, header, demographics, birthdate, facility_info, datim_code,
                      art_start_date, pregnancy_status, visit_date,
                      last_drug_pickup_date, last_regimen, days_of_arv_refill,
                      indication_for_viral_load_test, viral_load,
                      viral_load_sample_collection_date, viral_load_report_date,
                      status, visit_date_record):
    """Creates a flattened record with all viral load data"""

    # Get patient identifiers
    patient_identifier = demographicsutils.get_patient_identifier(4, doc)
    hospital_number = demographicsutils.get_patient_identifier(5, doc)
    patient_uuid = demographics.get("patientUuid")

    # Create record with all required headers (removed create_date and update_date)
    record = {
        "id": patient_uuid,
        "visit_date": visit_date,
        "state": facility_info.get("State") if facility_info else None,
        "lga": facility_info.get("LGA") if facility_info else None,
        "facility": header.get("facilityName"),
        "datim_code": datim_code,
        "sex": demographics.get("gender"),
        "patient_identifier": patient_identifier,
        "hospital_number": hospital_number,
        "date_of_birth": birthdate,
        "art_start_date": art_start_date,
        "last_drug_pickup_date": last_drug_pickup_date,
        "last_regimen": last_regimen,
        "days_of_arv_refill": days_of_arv_refill,
        "pregnancy_status": pregnancy_status,
        "indication_for_viral_load_test": indication_for_viral_load_test,
        "viral_load": viral_load,
        "viral_load_sample_collection_date": viral_load_sample_collection_date,
        "viral_load_report_date": viral_load_report_date,
        "status": status,
        "patient_uuid": patient_uuid,
    }
    return record


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
        filename = f"ViralLoadExtraction_{timestamp}.csv"

    return os.path.join(output_dir, filename)


def save_batch_to_csv(batch_data, full_path, write_header):
    """Writes a single batch of data to the CSV file."""
    df = pd.DataFrame(batch_data)
    # mode='a' means Append
    # header=write_header ensures the column names only appear at the top
    df.to_csv(full_path, mode='a', index=False, header=write_header)


def load_facility_cache(db, db_name=MONGO_DATABASE_NAME):
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


def get_all_viral_load_obs(doc, cutoff_datetime=None):
    """
    Returns all viral load observations for a patient up to the cutoff date,
    sorted from oldest to newest.
    """
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    cutoff_datetime = commonutils.normalize_clinical_date(cutoff_datetime)

    for obs in obs_list:
        # Check basic criteria: LAB form, VIRAL_LOAD concept, not voided
        if (obs.get("formId") == labutils.LAB_FORM_ID and
            obs.get("conceptId") == labutils.VIRAL_LOAD_CONCEPT_ID and
            obs.get("voided") == 0):

            # Normalize the observation date
            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))

            # Check if date is valid and within cutoff
            if isinstance(obs_dt, datetime) and obs_dt <= cutoff_datetime:
                matching_obs.append(obs)

    if not matching_obs:
        return []

    # Sort from oldest to newest
    matching_obs.sort(
        key=lambda x: commonutils.normalize_clinical_date(x.get('obsDatetime')) or datetime(1900, 1, 1)
    )

    return matching_obs


def get_pregnancy_status_for_encounter(doc, encounter_id):
    """
    Gets the pregnancy status (any value: pregnant, not pregnant, breastfeeding, blank, etc.)
    for a specific encounter from the pharmacy form.
    """
    if not encounter_id:
        return None
    
    obs_list = doc.get("messageData", {}).get("obs", [])
    
    for obs in obs_list:
        # Look for pregnancy status obs in pharmacy form with matching encounter
        if (obs.get("formId") == pharmacyutils.PHARMACY_FORM_ID and
            obs.get("conceptId") == pharmacyutils.PREGNANCY_STATUS_CONCEPT_ID and
            obs.get("encounterId") == encounter_id and
            obs.get("voided") == 0):
            
            # Return any value: pregnant, not pregnant, breastfeeding, blank, etc.
            pregnancy_value = obs.get("variableValue")
            return pregnancy_value
    
    return None


def is_obs_within_date_range(obs, start_date=None, end_date=None):
    """
    Checks if an observation falls within the specified date range.
    """
    obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
    
    if not isinstance(obs_dt, datetime):
        return False
    
    if start_date and obs_dt < start_date:
        return False
    
    if end_date and obs_dt > end_date:
        return False
    
    return True

