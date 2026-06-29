import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
from collections import OrderedDict
import os
import logging
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
ASPIRE_STATES = ["Nasarawa", "Rivers"]




def export_data(cutoff_datetime=None, filename=None):
    db_name=MONGO_DATABASE_NAME
    db = mongo_dao.get_db_connection(db_name)
    cursor, size = mongo_dao.get_containers_by_states(db=db, states_list=ASPIRE_STATES)
    # size = mongo_dao.get_art_container_size(db, db_name)
    # size=725000
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 1000
    batch_list = []

    cutoff_datetime = None # commonutils.normalize_clinical_date(datetime(2024, 10, 1)) if cutoff_datetime else None

    start_datetime = commonutils.normalize_clinical_date(datetime(2000, 10, 1))
    end_datetime = commonutils.normalize_clinical_date(datetime.now())
    # end_datetime = commonutils.normalize_clinical_date(datetime.now())

    # 1. Prepare the file path (create directory and name)
    full_path = prepare_filepath(filename)

    # Track if it's the first batch so we can write the CSV header
    is_first_batch = True

    #extracted_results = []
    for doc in tqdm(cursor, total=size, desc="Nas VL ETL Progress"):


        if not is_aspire_state(doc):
            continue  # Skip this record and move to the next one

        header = demographicsutils.get_message_header(doc)
        datim_code = header.get("facilityDatimCode")
        demographics = demographicsutils.get_patient_demographics(doc)
        birthdate = commonutils.normalize_clinical_date(demographics.get("birthdate"))
        facility_info = get_facility_by_datim(datim_code)
        art_start_date = commonutils.normalize_clinical_date(artcommence.get_art_start_date(doc, cutoff_datetime))

        last_marital_status = hivenrollmentutils.get_last_marital_status_obs(doc,cutoff_datetime).get('variableValue') if hivenrollmentutils.get_last_marital_status_obs(doc,cutoff_datetime) else None
        last_occupational_status = hivenrollmentutils.get_last_occupational_status_obs(doc,cutoff_datetime).get('variableValue') if hivenrollmentutils.get_last_occupational_status_obs(doc,cutoff_datetime) else None
        date_diagnosed = obsutils.getValueDatetimeFromObs(hivenrollmentutils.get_last_date_confirmed_hiv_positive_obs(doc,cutoff_datetime)) if hivenrollmentutils.get_last_date_confirmed_hiv_positive_obs(doc,cutoff_datetime) else None

        baseline_vl = labutils.get_first_viral_load_obs(doc, cutoff_datetime).get('variableValue') if labutils.get_first_viral_load_obs(doc, cutoff_datetime) else None
        baseline_vl_date = commonutils.normalize_datetime(labutils.get_first_viral_load_obs(doc,cutoff_datetime).get('obsDatetime')) if labutils.get_first_viral_load_obs(doc,cutoff_datetime) else None

        last_who_staging = carecardutils.get_last_who_stage_obs(doc, cutoff_datetime).get('variableValue') if carecardutils.get_last_who_stage_obs(doc, cutoff_datetime) else None
        last_tb_status = carecardutils.get_current_tb_status_obs(doc, cutoff_datetime).get('variableValue') if carecardutils.get_current_tb_status_obs(doc, cutoff_datetime) else None

        current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime)
        current_vl = obsutils.getValueNumericFromObs(current_viral_load_obs) if obsutils.getValueNumericFromObs(current_viral_load_obs) else None
        suppression_status = (1 if current_vl < 1000 else 0) if current_vl is not None else None

        first_suppressed_viral_load_obs = labutils.get_first_suppressed_viral_load_between_dates(doc, start_datetime, end_datetime)
        first_suppressed_viral_load_value = obsutils.getValueNumericFromObs(first_suppressed_viral_load_obs) if first_suppressed_viral_load_obs else None
        first_suppressed_viral_load_datetime = obsutils.getObsDatetimeFromObs(first_suppressed_viral_load_obs) if first_suppressed_viral_load_obs else None

        adherence_obs = carecardutils.get_nth_drug_adherence_obs_of_last_x_viral_loads(doc, 1, 1, cutoff_datetime) if carecardutils.get_nth_drug_adherence_obs_of_last_x_viral_loads(doc, 1, 1, cutoff_datetime) else None
        adherence = adherence_obs.get('variableValue') if adherence_obs is not None else None


        record = {
            # SECTION A: FACILITY IDENTIFIERS
            "State": facility_info.get("State") if facility_info else None,
            "LGA" : facility_info.get("LGA") if facility_info else None,
            "DatimCode" : header.get("facilityDatimCode"),
            "FacilityName": header.get("facilityName"),

            # SECTION B: PATIENT DEMOGRAPHICS
            "UniqueID": demographicsutils.get_patient_identifier(4, doc),
            "HospitalNumber": demographicsutils.get_patient_identifier(5, doc),
            "Sex": demographics.get("gender"),
            "AgeAtARTStartYears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
            "AgeAtARTStartMonths": demographicsutils.get_pediatric_age_art_start_months(doc, birthdate, art_start_date),
            "CurrentAgeYears": demographicsutils.get_current_age_at_date(doc,cutoff_datetime),
            "CurrentAgeMonths": demographicsutils.get_current_age_at_date_in_months(doc,cutoff_datetime),
            "DOB": birthdate,
            "MaritalStatus": last_marital_status,
            "OccupationalStatus": last_occupational_status,

            # SECTION C: CLINICAL AND TREATMENT DATA
            "DateDiagnosed": date_diagnosed,
            "ArtStartDate": art_start_date,
            "CurrentRegimenLine": pharmacyutils.get_current_regimen_line(doc,cutoff_datetime) ,
            "CurrentRegimen": pharmacyutils.get_current_regimen(doc,cutoff_datetime),
            "BaselineViralLoad": baseline_vl,
            "BaselineViralLoadEncounterDate": baseline_vl_date,
            # "BaselineViralLoadSampleDate": first_suppressed_viral_load_obs,
            "CurrentViralLoad": obsutils.getValueNumericFromObs(current_viral_load_obs),
            "ViralLoadEncounterDate": obsutils.getObsDatetimeFromObs(current_viral_load_obs),
            "ViralLoadSampleDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, current_viral_load_obs)),
            "ViralLoadIndication": obsutils.getVariableValueFromObs(labutils.get_viral_load_indication_obs_of_viral_load_obs(doc, current_viral_load_obs)),
            "ViralSuppressionStatus": suppression_status,
            "firstSuppressedViralLoad": first_suppressed_viral_load_value,
            "firstSuppressedViralLoadDate": first_suppressed_viral_load_datetime,
            "TimeToSuppression (months)": commonutils.get_month_diff(first_suppressed_viral_load_datetime, date_diagnosed),

            # SECTION D: PATIENT-RELATED FACTORS
            "MonthsOnArt": demographicsutils.get_months_on_art(doc,art_start_date,cutoff_datetime),
            "TBStatus": last_tb_status,
            "WHOStaging": last_who_staging,
            "Adherence": adherence ,
            
            # SECTION E: HEALTH SYSTEM FACTORS
            "PatientOutcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime),
            "PatientOutcomeDate" : ctdutils.get_outcome_date (doc,cutoff_datetime),


            "PatientUUID": demographicsutils.get_patient_demographics(doc).get("patientUuid"),





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
        filename = f"VLNasExport_{timestamp}.csv"

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

# check if document belongs to a facility in ASPIRE states (FCT,Katsina,Nasarawa,Rivers) ignore casing and whitespace
def is_aspire_state(doc):
    header = demographicsutils.get_message_header(doc)
    datim_code = header.get("facilityDatimCode")

    if not datim_code:
        return False
    facility = get_facility_by_datim(datim_code)

    if facility is None:
        return False
    if facility:
        state = facility.get("State", "").strip().upper()
        return state in ASPIRE_STATES
    return False




