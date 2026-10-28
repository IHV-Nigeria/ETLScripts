import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
import os
import logging
import dao.mongodbdao as mongo_dao
import utils.demographicutils as demographicsutils
import formslib.artcommencementutil as artcommence
import formslib.pharmacyutils as pharmacyutils
import utils.encounterutils as encounterutils
import utils.obsutils as obsutils
import utils.commonutils as commonutils
from dao.config import MONGO_DATABASE_NAME
from formslib import labutils
from legacy.constants import ctd_concepts
import formslib.ctdutils as ctdutils
import formslib.hivenrollmentutil as hivenrollmentutil
import formslib.carecardutils as carecardutils

ctd_form_id = ctd_concepts.get("Form_ID")
logger = logging.getLogger(__name__)


# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}
ASPIRE_STATES = {"FCT", "KATSINA", "NASARAWA", "RIVERS"}


# functions for DB insertion
def _to_naive_datetime(value):
    """
    Converts datetime/date values to naive datetime for safe comparisons.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    return None

def _resolve_cutoff_datetime(cutoff_datetime=None):
    if cutoff_datetime is None:
        return datetime.now()
    if isinstance(cutoff_datetime, str):
        return datetime.fromisoformat(cutoff_datetime.replace("Z", "+00:00"))
    return cutoff_datetime


def export_data(cutoff_datetime, filename=None):
    cutoff_datetime = _resolve_cutoff_datetime(cutoff_datetime)
    db_name=MONGO_DATABASE_NAME
    db = mongo_dao.get_db_connection(db_name)
    try:
        cursor, size = mongo_dao.get_ctd_containers(db, db_name)
        print(f"Processing {size} ART containers...")
        logger.info("Starting ART export with cutoff %s", cutoff_datetime)
        load_facility_cache(db, db_name)
        BATCH_SIZE = 1000
        batch_list = []

        full_path = prepare_filepath(filename)
        is_first_batch = True

        for doc in tqdm(cursor, total=size, desc="ETL Progress"):
            if not is_aspire_state(doc):
                continue

            header = demographicsutils.get_message_header(doc)
            datim_code = header.get("facilityDatimCode")
            demographics = demographicsutils.get_patient_demographics(doc)
            birthdate = commonutils.validate_date(demographics.get("birthdate"))
            facility_info = get_facility_by_datim(datim_code)
            art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
            age = demographicsutils.get_current_age_at_date(doc,cutoff_datetime)
            last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)

            current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime)
            City_Village = demographics.get("cityVillage")
            current_tb_obs = carecardutils.get_current_tb_status_obs(doc, cutoff_datetime)
            current_tb = obsutils.getVariableValueFromObs(current_tb_obs)


            record = {
                "Unique Study ID": demographicsutils.get_patient_demographics(doc).get("patientUuid"),
                "state": facility_info.get("State") if facility_info else None,
                "lga" : facility_info.get("LGA") if facility_info else None,
                "facility_name": header.get("facilityName"),
                "datim_code": header.get("facilityDatimCode"),
                "patient_id": demographicsutils.get_patient_identifier(4, doc),
                "hospital_no": demographicsutils.get_patient_identifier(5, doc),
                "art_start_date": art_start_date,
                "date_of_birth": birthdate,
                "age": age,

                #Section B
                "AgeAtARTStartYears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
                "AgeAtARTStartMonths": demographicsutils.get_pediatric_age_art_start_months(doc, birthdate, art_start_date),
                "sex": demographics.get("gender"),
                "marital_status": obsutils.getVariableValueFromObs(hivenrollmentutil.get_last_marital_status_obs(doc)),
                "occupation": obsutils.getVariableValueFromObs(hivenrollmentutil.get_last_occupational_status_obs(doc)),
                "education": obsutils.getVariableValueFromObs(hivenrollmentutil.get_last_education_level_obs(doc)),
                "residence": City_Village if City_Village and "NULL" not in City_Village else None,

                # Section C
                "functional_status": obsutils.getVariableValueFromObs(carecardutils.get_functional_status_obs(doc)),
                "who_staging": obsutils.getVariableValueFromObs(carecardutils.get_last_who_stage_obs(doc)),
                "first_vl": obsutils.getVariableValueFromObs(labutils.get_first_viral_load_obs(doc)),
                "CurrentViralLoad": obsutils.getValueNumericFromObs(current_viral_load_obs),
                "CurrentViralLoadEncounterDate": obsutils.getObsDatetimeFromObs(current_viral_load_obs),
                "CurrentViralLoadSampleDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, current_viral_load_obs)),
                "CurrentViralLoadIndication": obsutils.getVariableValueFromObs(labutils.get_viral_load_indication_obs_of_viral_load_obs(doc, current_viral_load_obs)),

                "tb_status": current_tb,
                "pregnancy_status": carecardutils.get_current_pregnancy_status(doc),
                "regimen_at_start": pharmacyutils.get_initial_regimen(doc),
                "regimen_line_at_start": pharmacyutils.get_initial_regimen_line(doc),
                "CurrentRegimenLine": pharmacyutils.get_current_regimen_line(doc,cutoff_datetime) ,
                "CurrentRegimen": pharmacyutils.get_current_regimen(doc,cutoff_datetime),
                "MonthsOnArt": demographicsutils.get_months_on_art(doc,art_start_date,cutoff_datetime),


                # Section D: Treatment Outcome Variables
                "LastPickupDate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
                "LastVisitDate": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
                "Status": pharmacyutils.get_current_art_status(doc, cutoff_datetime),
                "PatientOutcome" : ctdutils.get_patient_outcome(doc,cutoff_datetime),
                "PatientOutcomeDate" : ctdutils.get_outcome_date(doc,cutoff_datetime),

            }
            batch_list.append(record)

            if len(batch_list) >= BATCH_SIZE:
                save_batch_to_csv(batch_list, full_path, is_first_batch)
                batch_list = []
                is_first_batch = False

        if batch_list:
            save_batch_to_csv(batch_list, full_path, is_first_batch)

        print(f"\nFinal export complete. Total records processed: {size}")
        print(f"File saved to: {full_path}")
        logger.info("Completed data export to %s", full_path)
        return full_path
    finally:
        db.client.close()


def run_data_export(cutoff_datetime=None, filename=None):
    resolved_cutoff_datetime = _resolve_cutoff_datetime(cutoff_datetime)
    output_path = export_data(cutoff_datetime=resolved_cutoff_datetime, filename=filename)
    return {
        "output_path": output_path,
        "filename": os.path.basename(output_path),
        "cutoff_datetime": resolved_cutoff_datetime.isoformat(),
    }


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
        filename = f"CTDExport_{timestamp}.csv"

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
