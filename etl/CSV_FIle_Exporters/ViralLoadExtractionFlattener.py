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
ASPIRE_STATES = {"FCT", "KATSINA", "NASARAWA", "RIVERS"}


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

        if not is_aspire_state(facility_info):
            continue

        art_start_date = commonutils.normalize_clinical_date(artcommence.get_art_start_date(doc, cutoff_datetime))

        # Get pharmacy data (drug pickup and regimen)
        last_arv_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)
        last_drug_pickup_date = obsutils.getObsDatetimeFromObs(last_arv_obs) if last_arv_obs else None
        last_regimen = pharmacyutils.get_current_regimen(doc, cutoff_datetime)
        days_of_arv_refill = pharmacyutils.get_last_drug_pickup_duration(doc, last_arv_obs)

        # Populate first-to-fifth viral load columns with the shared labutils helpers.
        vl_data_list = get_first_five_viral_load_data(
            doc,
            cutoff_datetime=cutoff_datetime,
            start_date=start_date,
            end_date=end_date
        )

        # If no viral loads after filtering, skip this patient
        if not vl_data_list:
            continue

        # Get pregnancy status from the most recent encounter with a viral load
        pregnancy_status = None
        if vl_data_list:
            pregnancy_status = get_pregnancy_status_for_encounter(doc, vl_data_list[-1]["obs"].get('encounterId'))

        # Create one record per patient with first 5 viral loads in columns
        record = create_flattened_record(
            doc, header, demographics, birthdate, facility_info, datim_code,
            art_start_date, pregnancy_status,
            last_drug_pickup_date, last_regimen, days_of_arv_refill,
            vl_data_list
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


def create_flattened_record(doc, header, demographics, birthdate, facility_info, datim_code,
                         art_start_date, pregnancy_status,
                         last_drug_pickup_date, last_regimen, days_of_arv_refill,
                         vl_data_list):
    """Creates a flattened record with facility info, patient demographics, and first 5 viral loads in columns"""

    # Get patient identifiers
    patient_identifier = demographicsutils.get_patient_identifier(4, doc)
    hospital_number = demographicsutils.get_patient_identifier(5, doc)
    patient_uuid = demographics.get("patientUuid")

    # Base record with facility and demographics info
    record = {
        "id": patient_uuid,
        "patient_uuid": patient_uuid,
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
        "viral_load_1": "",
        "viral_load_1_sample_collection_date": "",
        "viral_load_1_report_date": "",
        "viral_load_1_indication": "",
        "viral_load_1_status": "",
        "viral_load_2": "",
        "viral_load_2_sample_collection_date": "",
        "viral_load_2_report_date": "",
        "viral_load_2_indication": "",
        "viral_load_2_status": "",
        "viral_load_3": "",
        "viral_load_3_sample_collection_date": "",
        "viral_load_3_report_date": "",
        "viral_load_3_indication": "",
        "viral_load_3_status": "",
        "viral_load_4": "",
        "viral_load_4_sample_collection_date": "",
        "viral_load_4_report_date": "",
        "viral_load_4_indication": "",
        "viral_load_4_status": "",
        "viral_load_5": "",
        "viral_load_5_sample_collection_date": "",
        "viral_load_5_report_date": "",
        "viral_load_5_indication": "",
        "viral_load_5_status": ""
    }

    # Add first 5 viral loads as columns
    for idx, vl_data in enumerate(vl_data_list, start=1):
        record[f"viral_load_{idx}"] = vl_data["viral_load"]
        record[f"viral_load_{idx}_sample_collection_date"] = vl_data["sample_collection_date"]
        record[f"viral_load_{idx}_report_date"] = vl_data["report_date"]
        record[f"viral_load_{idx}_indication"] = vl_data["indication"]
        record[f"viral_load_{idx}_status"] = vl_data["status"]

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


def is_blank_value(value):
    return value is None or (isinstance(value, str) and value.strip() == "")


def get_normalized_value_datetime(obs):
    if not obs:
        return None

    value_datetime = obsutils.getValueDatetimeFromObs(obs)
    if value_datetime is not None:
        return value_datetime

    raw_value_datetime = obs.get("valueDatetime")
    if isinstance(raw_value_datetime, dict):
        raw_value_datetime = raw_value_datetime.get("$date")

    return commonutils.normalize_datetime(raw_value_datetime)


def get_indication_value(obs):
    if not obs:
        return None

    for value in (
        obsutils.getVariableValueFromObs(obs),
        obsutils.getValueTextFromObs(obs),
        obsutils.getVariableNameFromObs(obs),
    ):
        if not is_blank_value(value):
            return value

    return None


def get_lab_obs_by_encounter_id(doc, encounter_id, concept_id):
    if not encounter_id:
        return None

    matching_obs = []
    for obs in doc.get("messageData", {}).get("obs", []):
        if (obs.get("formId") == labutils.LAB_FORM_ID and
                obs.get("conceptId") == concept_id and
                obs.get("encounterId") == encounter_id and
                obs.get("voided") == 0):
            matching_obs.append(obs)

    if not matching_obs:
        return None

    matching_obs.sort(
        key=lambda obs: commonutils.normalize_clinical_date(obs.get("obsDatetime")) or datetime(1900, 1, 1)
    )
    return matching_obs[0]



def build_viral_load_data(doc, viral_load_obs):
    if not viral_load_obs:
        return None

    viral_load = obsutils.getValueNumericFromObs(viral_load_obs)
    encounter_id = viral_load_obs.get("encounterId")
    sample_collection_obs = get_lab_obs_by_encounter_id(doc, encounter_id, labutils.SAMPLE_COLLECTION_DATE_CONCEPT_ID)
    reported_date_obs = get_lab_obs_by_encounter_id(doc, encounter_id, labutils.VIRAL_LOAD_REPORTED_DATE_CONCEPT_ID)
    indication_obs = get_lab_obs_by_encounter_id(doc, encounter_id, labutils.VIRAL_LOAD_INDICATION_CONCEPT_ID)

    return {
        "obs": viral_load_obs,
        "viral_load": viral_load,
        "sample_collection_date": get_normalized_value_datetime(sample_collection_obs),
        "report_date": get_normalized_value_datetime(reported_date_obs),
        "indication": get_indication_value(indication_obs),
        "status": "Suppressed" if viral_load is not None and viral_load < 1000
        else "Unsuppressed" if viral_load is not None else None
    }


def get_first_five_viral_load_data(doc, cutoff_datetime=None, start_date=None, end_date=None):
    viral_load_obs_list = get_all_viral_load_obs(doc, cutoff_datetime)
    effective_end_date = end_date

    if cutoff_datetime and effective_end_date:
        effective_end_date = min(cutoff_datetime, effective_end_date)
    elif cutoff_datetime:
        effective_end_date = cutoff_datetime

    filtered_viral_load_obs_list = [
        viral_load_obs for viral_load_obs in viral_load_obs_list
        if is_obs_within_date_range(viral_load_obs, start_date, effective_end_date)
    ]

    return [
        build_viral_load_data(doc, viral_load_obs)
        for viral_load_obs in filtered_viral_load_obs_list[:5]
    ]


def get_all_viral_load_obs(doc, cutoff_datetime=None):
    viral_load_obs_list = []

    for obs in doc.get("messageData", {}).get("obs", []):
        if (obs.get("formId") == labutils.LAB_FORM_ID and
                obs.get("conceptId") == labutils.VIRAL_LOAD_CONCEPT_ID and
                obs.get("voided") == 0):
            obs_datetime = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            if not isinstance(obs_datetime, datetime):
                continue
            if cutoff_datetime and obs_datetime > cutoff_datetime:
                continue
            viral_load_obs_list.append(obs)

    viral_load_obs_list.sort(
        key=lambda obs: commonutils.normalize_clinical_date(obs.get("obsDatetime")) or datetime(1900, 1, 1)
    )

    return viral_load_obs_list


def is_aspire_state(facility_info):
    if not facility_info:
        return False

    state = facility_info.get("State")
    if not isinstance(state, str):
        return False

    return state.strip().upper() in ASPIRE_STATES


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
