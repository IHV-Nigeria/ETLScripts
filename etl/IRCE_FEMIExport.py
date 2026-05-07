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
from formslib.pharmacyutils import (PHARMACY_FORM_ID, MEDICATION_QUANTITY_DISPENSED_CONCEPT_ID, NEXT_APPOINTMENT_DATE_CONCEPT_ID,
                                    CURRENT_REGIMEN_LINE_CONCEPT_ID, DRUG_REGIMEN_CONCEPT_LIST)
from legacy.constants import CARE_CARD_FORM_ID
from formslib.carecardutils import ARV_DRUGS_ADHERENCE_CONCEPT_ID

# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}



def export_request_data(cutoff_datetime=None, filename=None):
    db_name=MONGO_DATABASE_NAME
    db = mongo_dao.get_db_connection(db_name)
    #pepfarids=["RIV65721878","RIV65302342","RIV65300488","RIV57502335"]
    #pepfarids=['RIV65302342','RIV65300488','RIV57502335']
    cursor = mongo_dao.get_art_containers(db, db_name)
    #size = mongo_dao.get_art_container_size(db, db_name)
    size=725000
    #size=4
    #cutoff_datetime = commonutils.normalize_clinical_date(cutoff_datetime) if cutoff_datetime else None
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 1000
    batch_list = []

    cutoff_datetime = commonutils.normalize_clinical_date(datetime(2024, 10, 1)) if cutoff_datetime else None

    start_datetime = commonutils.normalize_clinical_date(datetime(2024, 10, 1))
    # end_datetime = commonutils.normalize_clinical_date(datetime.now())
    end_datetime = commonutils.normalize_clinical_date(datetime.now())

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

        obs_in_group1 = obsutils.get_arv_obs_rev(doc, formid=PHARMACY_FORM_ID, conceptid=MEDICATION_QUANTITY_DISPENSED_CONCEPT_ID, n=1)
        obs_in_group2 = obsutils.get_arv_obs_rev(doc, formid=PHARMACY_FORM_ID, conceptid=MEDICATION_QUANTITY_DISPENSED_CONCEPT_ID, n=2)
        obs_in_group3 = obsutils.get_arv_obs_rev(doc, formid=PHARMACY_FORM_ID, conceptid=MEDICATION_QUANTITY_DISPENSED_CONCEPT_ID, n=3)
        obs_in_group4 = obsutils.get_arv_obs_rev(doc, formid=PHARMACY_FORM_ID, conceptid=MEDICATION_QUANTITY_DISPENSED_CONCEPT_ID, n=4)
        obs_in_group5 = obsutils.get_arv_obs_rev(doc, formid=PHARMACY_FORM_ID, conceptid=MEDICATION_QUANTITY_DISPENSED_CONCEPT_ID, n=5)

        next_visit_1_encounter_id = obs_in_group1.get('encounterId') if obs_in_group1 else None
        next_visit_2_encounter_id = obs_in_group2.get('encounterId') if obs_in_group2 else None
        next_visit_3_encounter_id = obs_in_group3.get('encounterId') if obs_in_group3 else None
        next_visit_4_encounter_id = obs_in_group4.get('encounterId') if obs_in_group4 else None
        next_visit_5_encounter_id = obs_in_group5.get('encounterId') if obs_in_group5 else None

        next_visit_1_visit_uuid = obs_in_group1.get('visitUuid') if obs_in_group1 else None
        next_visit_2_visit_uuid = obs_in_group2.get('visitUuid') if obs_in_group2 else None
        next_visit_3_visit_uuid = obs_in_group3.get('visitUuid') if obs_in_group3 else None
        next_visit_4_visit_uuid = obs_in_group4.get('visitUuid') if obs_in_group4 else None
        next_visit_5_visit_uuid = obs_in_group5.get('visitUuid') if obs_in_group5 else None

        next_visit_in_group1 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id=next_visit_1_encounter_id)
        next_visit_in_group2 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id=next_visit_2_encounter_id)
        next_visit_in_group3 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id=next_visit_3_encounter_id)
        next_visit_in_group4 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id=next_visit_4_encounter_id)
        next_visit_in_group5 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id=next_visit_5_encounter_id)

        regimen_line_in_group1 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=CURRENT_REGIMEN_LINE_CONCEPT_ID, encounter_id=next_visit_1_encounter_id)
        regimen_line_in_group2 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=CURRENT_REGIMEN_LINE_CONCEPT_ID, encounter_id=next_visit_2_encounter_id)
        regimen_line_in_group3 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=CURRENT_REGIMEN_LINE_CONCEPT_ID, encounter_id=next_visit_3_encounter_id)
        regimen_line_in_group4 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=CURRENT_REGIMEN_LINE_CONCEPT_ID, encounter_id=next_visit_4_encounter_id)
        regimen_line_in_group5 = obsutils.get_obs_by_encounterid(doc, formid=PHARMACY_FORM_ID, conceptid=CURRENT_REGIMEN_LINE_CONCEPT_ID, encounter_id=next_visit_5_encounter_id)

        drug_in_group1 = obsutils.get_obs_by_encounterid_and_concept_list(doc, formid=PHARMACY_FORM_ID, conceptid=DRUG_REGIMEN_CONCEPT_LIST, encounter_id=next_visit_1_encounter_id)
        drug_in_group2 = obsutils.get_obs_by_encounterid_and_concept_list(doc, formid=PHARMACY_FORM_ID, conceptid=DRUG_REGIMEN_CONCEPT_LIST, encounter_id=next_visit_2_encounter_id)
        drug_in_group3 = obsutils.get_obs_by_encounterid_and_concept_list(doc, formid=PHARMACY_FORM_ID, conceptid=DRUG_REGIMEN_CONCEPT_LIST, encounter_id=next_visit_3_encounter_id)
        drug_in_group4 = obsutils.get_obs_by_encounterid_and_concept_list(doc, formid=PHARMACY_FORM_ID, conceptid=DRUG_REGIMEN_CONCEPT_LIST, encounter_id=next_visit_4_encounter_id)
        drug_in_group5 = obsutils.get_obs_by_encounterid_and_concept_list(doc, formid=PHARMACY_FORM_ID, conceptid=DRUG_REGIMEN_CONCEPT_LIST, encounter_id=next_visit_5_encounter_id)

        adherence_1 = obsutils.get_obs_by_visit_uuid(doc, formid=CARE_CARD_FORM_ID, conceptid=ARV_DRUGS_ADHERENCE_CONCEPT_ID, visit_id=next_visit_1_visit_uuid)
        adherence_2 = obsutils.get_obs_by_visit_uuid(doc, formid=CARE_CARD_FORM_ID, conceptid=ARV_DRUGS_ADHERENCE_CONCEPT_ID, visit_id=next_visit_2_visit_uuid)
        adherence_3 = obsutils.get_obs_by_visit_uuid(doc, formid=CARE_CARD_FORM_ID, conceptid=ARV_DRUGS_ADHERENCE_CONCEPT_ID, visit_id=next_visit_3_visit_uuid)
        adherence_4 = obsutils.get_obs_by_visit_uuid(doc, formid=CARE_CARD_FORM_ID, conceptid=ARV_DRUGS_ADHERENCE_CONCEPT_ID, visit_id=next_visit_4_visit_uuid)
        Adherence_5 = obsutils.get_obs_by_visit_uuid(doc, formid=CARE_CARD_FORM_ID, conceptid=ARV_DRUGS_ADHERENCE_CONCEPT_ID, visit_id=next_visit_5_visit_uuid)

        last_vl = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime)
        second_last_vl = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, n=2, x=5, cutoff_datetime=cutoff_datetime)
        third_last_vl = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, n=3, x=5, cutoff_datetime=cutoff_datetime)
        fourth_last_vl = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, n=4, x=5, cutoff_datetime=cutoff_datetime)
        fifth_last_vl = labutils.get_nth_viral_load_obs_of_last_x_viral_loads(doc, n=5, x=5, cutoff_datetime=cutoff_datetime)



        current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime)
        last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)


        record = {
            "touchtime": header.get("touchTime"),
            "PatientUUID": demographicsutils.get_patient_demographics(doc).get("patientUuid"),
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

            "TBStatus":carecardutils.get_current_tb_status_obs(doc, cutoff_datetime).get("variableValue") if carecardutils.get_current_tb_status_obs(doc, cutoff_datetime) else None,
            "Weight":carecardutils.get_current_weight_obs(doc, cutoff_datetime).get("variableValue") if carecardutils.get_current_weight_obs(doc, cutoff_datetime) else None,
            "FunctionalStatus":carecardutils.get_functional_status_obs(doc, cutoff_datetime).get("variableValue") if carecardutils.get_functional_status_obs(doc, cutoff_datetime) else None,
            "WHOStaging":carecardutils.get_last_who_stage_obs(doc, cutoff_datetime).get("variableValue") if carecardutils.get_last_who_stage_obs(doc, cutoff_datetime) else None,
            "CD4": labutils.get_current_cd4_count_obs(doc, cutoff_datetime).get("variableValue") if labutils.get_current_cd4_count_obs(doc, cutoff_datetime) else None,

            # Get Last 5
            "LastVL": last_vl.get("variableValue") if last_vl else None,
            "LastVLDate": last_vl.get("obsDatetime") if last_vl else None,
            "SecondLastVL": second_last_vl.get("variableValue") if second_last_vl else None,
            "SecondLastVLDate": second_last_vl.get("obsDatetime") if second_last_vl else None,
            "ThirdLastVL": third_last_vl.get("variableValue") if third_last_vl else None,
            "ThirdLastVLDate": third_last_vl.get("obsDatetime") if third_last_vl else None,
            "FourthLastVL": fourth_last_vl.get("variableValue") if fourth_last_vl else None,
            "FourthLastVLDate": fourth_last_vl.get("obsDatetime") if fourth_last_vl else None,
            "FifthLastVL": fifth_last_vl.get("variableValue") if fifth_last_vl else None,
            "FifthLastVLDate": fifth_last_vl.get("obsDatetime") if fifth_last_vl else None,

            # Get Last 5
            "last_visit5": obs_in_group5.get('obsDatetime') if obs_in_group5 else None,
            "regimen5": regimen_line_in_group5.get('variableValue') if regimen_line_in_group5 else None,
            "drug5": drug_in_group5.get('variableValue') if drug_in_group5 else None,
            "adherence5": Adherence_5.get('variableValue') if Adherence_5 else None,
            "duration5": obs_in_group5.get('valueNumeric') if obs_in_group5 else None,
            "next_app5": next_visit_in_group5.get('valueDatetime') if next_visit_in_group5 else None,

            "last_visit4": obs_in_group4.get('obsDatetime') if obs_in_group4 else None,
            "regimen4": regimen_line_in_group4.get('variableValue') if regimen_line_in_group4 else None,
            "drug4": drug_in_group4.get('variableValue') if drug_in_group4 else None,
            "adherence4": adherence_4.get('variableValue') if adherence_4 else None,
            "duration4": obs_in_group4.get('valueNumeric') if obs_in_group4 else None,
            "next_app4": next_visit_in_group4.get('valueDatetime') if next_visit_in_group4 else None,

            "last_visit3": obs_in_group3.get('obsDatetime') if obs_in_group3 else None,
            "regimen3": regimen_line_in_group3.get('variableValue') if regimen_line_in_group3 else None,
            "drug3": drug_in_group3.get('variableValue') if drug_in_group3 else None,
            "adherence3": adherence_3.get('variableValue') if adherence_3 else None,
            "duration3": obs_in_group3.get('valueNumeric') if obs_in_group3 else None,
            "next_app3": next_visit_in_group3.get('valueDatetime') if next_visit_in_group3 else None,

            "last_visit2": obs_in_group2.get('obsDatetime') if obs_in_group2 else None,
            "regimen2": regimen_line_in_group2.get('variableValue') if regimen_line_in_group2 else None,
            "drug2": drug_in_group2.get('variableValue') if drug_in_group2 else None,
            "adherence2": adherence_2.get('variableValue') if adherence_2 else None,
            "duration2": obs_in_group2.get('valueNumeric') if obs_in_group2 else None,
            "next_app2": next_visit_in_group2.get('valueDatetime') if next_visit_in_group2 else None,

            "last_visit1": obs_in_group1.get('obsDatetime') if obs_in_group1 else None,
            "regimen1": regimen_line_in_group1.get('variableValue') if regimen_line_in_group1 else None,
            "drug1": drug_in_group1.get('variableValue') if drug_in_group1 else None,
            "adherence1": adherence_1.get('variableValue') if adherence_1 else None,
            "duration1": obs_in_group1.get('valueNumeric') if obs_in_group1 else None,
            "next_app1": next_visit_in_group1.get('valueDatetime') if next_visit_in_group1 else None,


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

