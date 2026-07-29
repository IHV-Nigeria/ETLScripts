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
from legacy.constants import ctd_concepts

ctd_form_id = ctd_concepts.get("Form_ID")


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

def _extract_upsert_doc_key(doc):
    header = demographicsutils.get_message_header(doc)
    demographics = demographicsutils.get_patient_demographics(doc)
    return {
        "patientuuid": demographics.get("patientUuid"),
        "datimcode": header.get("facilityDatimCode"),
        "touchtime": _to_naive_datetime(header.get("touchTime"))
    }

def _prefilter_stale_docs_before_conversion(doc_batch, conn, return_valid_docs=True, touchtime_cache=None, cache_max_size=50000):
    """
    Prefilters stale docs in batch before expensive conversion by comparing touchtime.
    """
    keyed_docs = []
    invalid_key_count = 0

    for doc in doc_batch:
        key_data = _extract_upsert_doc_key(doc)
        patientuuid = key_data.get("patientuuid")
        datimcode = key_data.get("datimcode")
        if not patientuuid or not datimcode:
            invalid_key_count += 1
            continue
        keyed_docs.append((doc, patientuuid, datimcode, key_data.get("touchtime")))

    if not keyed_docs:
        return [], {"stale": 0, "invalid": invalid_key_count, "valid": 0}

    key_pairs = [(item[1], item[2]) for item in keyed_docs]
    existing_touchtime_map = {}

    # Query only keys not present in cache to reduce DB round-trips on duplicate keys.
    missing_key_pairs = []
    seen_missing = set()
    for patientuuid, datimcode in key_pairs:
        key_tuple = (patientuuid, datimcode)
        if touchtime_cache is not None and key_tuple in touchtime_cache:
            if isinstance(touchtime_cache, OrderedDict):
                touchtime_cache.move_to_end(key_tuple)
            existing_touchtime_map[key_tuple] = touchtime_cache[key_tuple]
            continue
        if key_tuple not in seen_missing:
            seen_missing.add(key_tuple)
            missing_key_pairs.append(key_tuple)

    if missing_key_pairs:
        fetched_map = postgres_dao.get_art_line_list_existing_touchtimes(conn, missing_key_pairs)
        for key_tuple in missing_key_pairs:
            cached_value = _to_naive_datetime(fetched_map.get(key_tuple))
            existing_touchtime_map[key_tuple] = cached_value
            if touchtime_cache is not None:
                touchtime_cache[key_tuple] = cached_value
                if isinstance(touchtime_cache, OrderedDict):
                    touchtime_cache.move_to_end(key_tuple)
                if len(touchtime_cache) > cache_max_size:
                    touchtime_cache.popitem(last=False)

    stale_count = 0
    valid_count = 0
    valid_docs = [] if return_valid_docs else None
    for doc, patientuuid, datimcode, incoming_touchtime in keyed_docs:
        key_tuple = (patientuuid, datimcode)
        existing_touchtime = _to_naive_datetime(existing_touchtime_map.get(key_tuple))

        # First-time records (not found in PostgreSQL) must always pass to upsert.
        if existing_touchtime is None:
            valid_count += 1
            if return_valid_docs:
                valid_docs.append(doc)
            continue

        if existing_touchtime is not None and incoming_touchtime is not None and incoming_touchtime <= existing_touchtime:
            stale_count += 1
            continue

        valid_count += 1
        if return_valid_docs:
            valid_docs.append(doc)

    return valid_docs or [], {"stale": stale_count, "invalid": invalid_key_count, "valid": valid_count}

def upsert_art_line_list_data(cutoff_datetime=None):
    logging.basicConfig(filename='etl_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

    db_name=MONGO_DATABASE_NAME
    #datims = ["LmLBtmd8U43"]
    db = mongo_dao.get_db_connection(db_name)
    size = mongo_dao.get_art_container_size(db,db_name)
    #cursor = mongo_dao.get_containers_by_datim_list(db,datims,db_name)
    #size = mongo_dao.get_container_by_datim_list_size(db,datims,db_name)
    conn=postgres_dao.connect_to_postgresqldb()
    if conn is None:
        print("Failed to connect to PostgreSQL. Data not saved.")
        return
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 2000 # Increased for 700k records
    doc_batch = []
    inserted_count = 0
    updated_count = 0
    skipped_count = 0
    prefilter_stale_count = 0
    prefilter_invalid_key_count = 0
    valid_doc_count = 0
    error_count = 0

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()


    try:
        # Phase 1: count valid/stale/invalid docs without holding valid docs in memory.
        cursor_phase_1 = mongo_dao.get_art_containers(db, db_name)[0]
        for doc in tqdm(cursor_phase_1, total=size, desc="Phase 1/2 - Filtering docs", unit="doc"):
            try:
                if not is_aspire_state(doc):
                    continue

                doc_batch.append(doc)
                if len(doc_batch) >= BATCH_SIZE:
                    _, prefilter_result = _prefilter_stale_docs_before_conversion(doc_batch, conn, return_valid_docs=False)
                    prefilter_stale_count += prefilter_result.get('stale', 0)
                    prefilter_invalid_key_count += prefilter_result.get('invalid', 0)
                    valid_doc_count += prefilter_result.get('valid', 0)
                    doc_batch.clear()
            except Exception as e:
                logging.error(f"Error during filtering phase: {e}")
                error_count += 1
                continue

        if doc_batch:
            _, prefilter_result = _prefilter_stale_docs_before_conversion(doc_batch, conn, return_valid_docs=False)
            prefilter_stale_count += prefilter_result.get('stale', 0)
            prefilter_invalid_key_count += prefilter_result.get('invalid', 0)
            valid_doc_count += prefilter_result.get('valid', 0)
            doc_batch.clear()

        # Phase 2: re-read cursor, stream valid docs and upsert in batches.
        cursor_phase_2 = mongo_dao.get_art_containers(db, db_name)[0]
        phase_2_touchtime_cache = OrderedDict()
        with tqdm(total=valid_doc_count, desc="Phase 2/2 - Upserting valid docs", unit="doc") as upsert_progress:
            for doc in cursor_phase_2:
                try:
                    if not is_aspire_state(doc):
                        continue

                    doc_batch.append(doc)
                    if len(doc_batch) < BATCH_SIZE:
                        continue

                    valid_batch_docs, _ = _prefilter_stale_docs_before_conversion(
                        doc_batch,
                        conn,
                        return_valid_docs=True,
                        touchtime_cache=phase_2_touchtime_cache,
                    )
                    doc_batch.clear()
                    if not valid_batch_docs:
                        continue

                    batch_list = [convert_doc_to_record(valid_doc, cutoff_datetime) for valid_doc in valid_batch_docs]
                    if batch_list:
                        result_arr = postgres_dao.batch_upsert_art_line_list(conn, batch_list)
                        inserted_count += result_arr.get('inserted', 0)
                        updated_count += result_arr.get('updated', 0)
                        skipped_count += result_arr.get('skipped', 0)
                    upsert_progress.update(len(valid_batch_docs))
                except Exception as e:
                    logging.error(f"Error during upsert phase: {e}")
                    error_count += 1
                    doc_batch.clear()
                    continue

            if doc_batch:
                try:
                    valid_batch_docs, _ = _prefilter_stale_docs_before_conversion(
                        doc_batch,
                        conn,
                        return_valid_docs=True,
                        touchtime_cache=phase_2_touchtime_cache,
                    )
                    doc_batch.clear()
                    if valid_batch_docs:
                        batch_list = [convert_doc_to_record(valid_doc, cutoff_datetime) for valid_doc in valid_batch_docs]
                        if batch_list:
                            result_arr = postgres_dao.batch_upsert_art_line_list(conn, batch_list)
                            inserted_count += result_arr.get('inserted', 0)
                            updated_count += result_arr.get('updated', 0)
                            skipped_count += result_arr.get('skipped', 0)
                        upsert_progress.update(len(valid_batch_docs))
                except Exception as e:
                    logging.error(f"Error during final upsert batch: {e}")
                    error_count += 1
    except Exception as e:
        logging.error(f"Critical error in upsert_art_line_list_data: {e}")

    finally:
        # ALWAYS close connections
        conn.close()
        # db.client.close() # Depending on your mongo_dao implementation
        total_skipped = skipped_count + prefilter_stale_count + prefilter_invalid_key_count
        print(f"\nETL Complete. Records Skipped: {total_skipped}. Records Inserted: {inserted_count}. Records Updated: {updated_count}")
        print(f"Prefiltered stale docs: {prefilter_stale_count}. Prefiltered invalid keys: {prefilter_invalid_key_count}. Upsert-level skips: {skipped_count}")
        print(f"Valid docs passed to upsert phase: {valid_doc_count}")
        print(f"Total batch errors during processing: {error_count}")

    print(f"\nBatch insert to postgresql completed. Total records processed: {size}")

def initialize_eac_line_list_data(cutoff_datetime=None):


    db_name=MONGO_DATABASE_NAME
    db = mongo_dao.get_db_connection(db_name)
    cursor, size = mongo_dao.get_art_containers(db,db_name)
    # size = mongo_dao.get_art_container_size(db,db_name)
    conn=postgres_dao.connect_to_postgresqldb()
    if conn is None:
        print("Failed to connect to PostgreSQL. Data not saved.")
        return
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 2000 # Increased for 700k records
    batch_list = []
    total_inserted = 0

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    previous_quarter_end_date = commonutils.get_previous_quarter_end_date(cutoff_datetime)


    try:
        #extracted_results = []
        for doc in tqdm(cursor, total=size, desc="EAC Line List ETL Progress"):


            if not is_aspire_state(doc):
                continue  # Skip this record and move to the next one

            record = convert_doc_to_record(doc, cutoff_datetime)
            batch_list.append(record)

            if len(batch_list) >= BATCH_SIZE:
                postgres_dao.save_to_postgres(conn, "eac_line_list", batch_list)
                total_inserted += len(batch_list)
                batch_list.clear() # clear() is slightly more memory efficient than []


        # Final Batch
        if batch_list:
            postgres_dao.save_to_postgres(conn, "eac_line_list", batch_list)
            total_inserted += len(batch_list)

    except Exception as e:
        print(f"Critical error during ETL: {e}")
        conn.rollback()
    finally:
        # ALWAYS close connections
        conn.close()
        # db.client.close() # Depending on your mongo_dao implementation
        print(f"\nETL Complete. Records Skipped:  Records Inserted: {total_inserted}")

    print(f"\nBatch insert to postgresql completed. Total records processed: {size}")

def convert_doc_to_record(doc, cutoff_datetime):
    """
    Converts a MongoDB document to a dictionary record for PostgreSQL insertion.
    This function is called for each document in the cursor.
    """
    # Extract necessary fields from the document using utility functions
    # and construct the record dictionary as shown in the main loop.
    # For brevity, this function is not fully implemented here, but it would
    # contain the same logic as the record construction in the main loop of initialize_art_line_list_data.
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    start_datetime = commonutils.normalize_clinical_date(datetime(2024, 10, 1))
    end_datetime = commonutils.normalize_clinical_date(datetime.now())


    header = demographicsutils.get_message_header(doc)
    datim_code = header.get("facilityDatimCode")
    demographics = demographicsutils.get_patient_demographics(doc)
    birthdate = commonutils.validate_date(demographics.get("birthdate"))
    facility_info = get_facility_by_datim(datim_code)
    art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
    last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)

    last_tracking_encounter = encounterutils.get_last_encounter_date_by_form_id(doc, form_id=ctd_form_id)
    # last_tracking_encounter_date = encounterutils.get_encounter_datetime(last_tracking_encounter)
    reason_for_tracking_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason for Tracking"))
    reason_for_tracking = obsutils.getVariableValueFromObs(reason_for_tracking_obs)
    Date_of_Last_Actual_Contact_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Date of Last Actual Contact/ Appointment"))
    Date_of_Last_Actual_Contact = obsutils.getVariableValueFromObs(Date_of_Last_Actual_Contact_obs)
    date_of_missed_schedule = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Date of Missed Scheduled Appointment"))
    Date_of_Missed_Scheduled_Appointment = obsutils.getVariableValueFromObs(date_of_missed_schedule)
    client_verification_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Client Verification"))
    Client_Verification = obsutils.getVariableValueFromObs(client_verification_obs)
    indication_for_client_verification_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Indication for Client Verification"))
    Indication_for_Client_Verification = obsutils.getVariableValueFromObs(indication_for_client_verification_obs)
    patient_care_discontinued_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Patient Care in Facility Discontinued"))
    Patient_Care_in_Facility_Discontinued = obsutils.getVariableValueFromObs(patient_care_discontinued_obs)
    date_of_discontinuation_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Date of Discontinuation"))
    Date_of_Discontinuation = obsutils.getVariableValueFromObs(date_of_discontinuation_obs)
    reason_for_discontinuation_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason for Discontinuation"))
    Reason_for_Discontinuation = obsutils.getVariableValueFromObs(reason_for_discontinuation_obs)
    facility_transferred_to_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Facility transferred to"))
    Facility_transferred_to = obsutils.getVariableValueFromObs(facility_transferred_to_obs)
    cause_of_death_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Cause of Death"))
    Cause_of_Death = obsutils.getVariableValueFromObs(cause_of_death_obs)
    va_cause_of_death_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("VA Cause of Death"))
    VA_Cause_of_Death = obsutils.getVariableValueFromObs(va_cause_of_death_obs)
    adult_causes_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Adult Causes"))
    Adult_Causes = obsutils.getVariableValueFromObs(adult_causes_obs)
    child_causes_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Child Causes"))
    Child_Causes = obsutils.getVariableValueFromObs(child_causes_obs)
    other_cause_of_death_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Other cause of death"))
    Other_cause_of_death = obsutils.getVariableValueFromObs(other_cause_of_death_obs)
    reason_to_discontinue_care_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason to Discontinue Care"))
    Reason_to_Discontinue_Care = obsutils.getVariableValueFromObs(reason_to_discontinue_care_obs)
    discontinue_care_other_specify_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Discontinue Care other specify"))
    Discontinue_Care_other_specify = obsutils.getVariableValueFromObs(discontinue_care_other_specify_obs)
    date_of_lost_to_follow_up_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Date of Lost to follow up"))
    Date_of_Lost_to_follow_up = obsutils.getVariableValueFromObs(date_of_lost_to_follow_up_obs)
    reason_for_lost_to_follow_up_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason for Lost to follow up"))
    Reason_for_Lost_to_follow_up = obsutils.getVariableValueFromObs(reason_for_lost_to_follow_up_obs)
    reason_for_lost_to_follow_up_other_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason for Lost to follow up_Other"))
    Reason_for_Lost_to_follow_up_Other = obsutils.getVariableValueFromObs(reason_for_lost_to_follow_up_other_obs)


    record = {

        "uuid": demographicsutils.get_patient_demographics(doc).get("patientUuid"),
        "state": facility_info.get("State") if facility_info else None,
        "lga" : facility_info.get("LGA") if facility_info else None,
        "facility_name": header.get("facilityName"),
        "datim_code": header.get("facilityDatimCode"),
        "patient_id": demographicsutils.get_patient_identifier(4, doc),
        "hospital_no": demographicsutils.get_patient_identifier(5, doc),

        # "dob": birthdate,
        # "sex": demographics.get("gender"),
        # "art_start_date": art_start_date,
        # "last_pickup_date": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
        # "days_of_arv_pickup": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
        # "pill_balance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),

        "Date of Tracking": last_tracking_encounter,
        "Reason for Tracking": reason_for_tracking,
        # "Guardian / Treatment Partner's Name": "",
        # "Guardian / Treatment Partner's Contact Address": "",
        # "Guardian / Treatment Partner's Phone Number": "",
        "Date of Last Actual Contact/ Appointment": Date_of_Last_Actual_Contact,
        "Date of Missed Scheduled Appointment": Date_of_Missed_Scheduled_Appointment,
        "Client Verification": Client_Verification,
        "Indication for Client Verification": Indication_for_Client_Verification,
        "Patient Care in Facility Discontinued": Patient_Care_in_Facility_Discontinued,
        "Date of Discontinuation": Date_of_Discontinuation,
        "Reason for Discontinuation": Reason_for_Discontinuation,
        "Facility transferred to": Facility_transferred_to,
        "Cause of Death": Cause_of_Death,
        "VA Cause of Death": VA_Cause_of_Death,
        "Adult Causes": Adult_Causes,
        "Child Causes": Child_Causes,
        "Other cause of death": Other_cause_of_death,
        "Reason to Discontinue Care": Reason_to_Discontinue_Care,
        "Discontinue Care other specify": Discontinue_Care_other_specify,
        "Date of Lost to follow up": Date_of_Lost_to_follow_up,
        "Reason for Lost to follow up": Reason_for_Lost_to_follow_up,
        "Reason for Lost to follow up_Other": Reason_for_Lost_to_follow_up_Other,

    }
    return record




def export_ctd_data(cutoff_datetime=None, filename=None):
    db_name=MONGO_DATABASE_NAME
    db = mongo_dao.get_db_connection(db_name)
    cursor, size = mongo_dao.get_art_containers(db, db_name)
    #size = mongo_dao.get_art_container_size(db, db_name)
    # size=725000
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 1000
    batch_list = []

    # cutoff_datetime = commonutils.normalize_clinical_date(datetime(2024, 10, 1)) if cutoff_datetime else None

    start_datetime = commonutils.normalize_clinical_date(datetime(2024, 10, 1))
    # end_datetime = commonutils.normalize_clinical_date(datetime.now())
    end_datetime = commonutils.normalize_clinical_date(datetime.now())

    # 1. Prepare the file path (create directory and name)
    full_path = prepare_filepath(filename)

    # Track if it's the first batch so we can write the CSV header
    is_first_batch = True

    #extracted_results = []
    for doc in tqdm(cursor, total=size, desc="CTD ETL Progress"):


        if not is_aspire_state(doc):
            continue  # Skip this record and move to the next one

        header = demographicsutils.get_message_header(doc)
        datim_code = header.get("facilityDatimCode")
        demographics = demographicsutils.get_patient_demographics(doc)
        birthdate = commonutils.validate_date(demographics.get("birthdate"))
        facility_info = get_facility_by_datim(datim_code)
        art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
        last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)

        last_ctd_encounter = encounterutils.get_last_encounter_date_by_form_id(doc, form_id=ctd_form_id)
        # last_ctd_encounter_date = encounterutils.get_encounter_datetime(last_ctd_encounter) if last_ctd_encounter else None
        reason_for_tracking_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason for Tracking"))
        reason_for_tracking = obsutils.getVariableValueFromObs(reason_for_tracking_obs)
        Date_of_Last_Actual_Contact_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Date of Last Actual Contact/ Appointment"))
        Date_of_Last_Actual_Contact = obsutils.getValueDatetimeFromObs(Date_of_Last_Actual_Contact_obs) if Date_of_Last_Actual_Contact_obs else None
        date_of_missed_schedule = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Date of Missed Scheduled Appointment"))
        Date_of_Missed_Scheduled_Appointment = obsutils.getValueDatetimeFromObs(date_of_missed_schedule) if date_of_missed_schedule else None
        client_verification_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Client Verification"))
        Client_Verification = obsutils.getVariableValueFromObs(client_verification_obs)
        indication_for_client_verification_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Indication for Client Verification"))
        Indication_for_Client_Verification = obsutils.getVariableValueFromObs(indication_for_client_verification_obs)
        patient_care_discontinued_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Patient Care in Facility Discontinued"))
        Patient_Care_in_Facility_Discontinued = obsutils.getVariableValueFromObs(patient_care_discontinued_obs)
        date_of_discontinuation_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Date of Discontinuation"))
        Date_of_Discontinuation = obsutils.getValueDatetimeFromObs(date_of_discontinuation_obs) if date_of_discontinuation_obs else None
        reason_for_discontinuation_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason for Discontinuation"))
        Reason_for_Discontinuation = obsutils.getVariableValueFromObs(reason_for_discontinuation_obs)
        facility_transferred_to_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Facility transferred to"))
        Facility_transferred_to = obsutils.getVariableValueFromObs(facility_transferred_to_obs)
        cause_of_death_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Cause of Death"))
        Cause_of_Death = obsutils.getVariableValueFromObs(cause_of_death_obs)
        va_cause_of_death_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("VA Cause of Death"))
        VA_Cause_of_Death = obsutils.getVariableValueFromObs(va_cause_of_death_obs)
        adult_causes_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Adult Causes"))
        Adult_Causes = obsutils.getVariableValueFromObs(adult_causes_obs)
        child_causes_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Child Causes"))
        Child_Causes = obsutils.getVariableValueFromObs(child_causes_obs)
        other_cause_of_death_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Other cause of death"))
        Other_cause_of_death = obsutils.getVariableValueFromObs(other_cause_of_death_obs)
        reason_to_discontinue_care_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason to Discontinue Care"))
        Reason_to_Discontinue_Care = obsutils.getVariableValueFromObs(reason_to_discontinue_care_obs)
        discontinue_care_other_specify_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Discontinue Care other specify"))
        Discontinue_Care_other_specify = obsutils.getVariableValueFromObs(discontinue_care_other_specify_obs)
        date_of_lost_to_follow_up_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Date of Lost to follow up"))
        Date_of_Lost_to_follow_up = obsutils.getValueDatetimeFromObs(date_of_lost_to_follow_up_obs) if date_of_lost_to_follow_up_obs else None
        reason_for_lost_to_follow_up_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason for Lost to follow up"))
        Reason_for_Lost_to_follow_up = obsutils.getVariableValueFromObs(reason_for_lost_to_follow_up_obs)
        reason_for_lost_to_follow_up_other_obs = obsutils.get_last_obs_before_date(doc, form_id=ctd_form_id, concept_id=ctd_concepts.get("Reason for Lost to follow up_Other"))
        Reason_for_Lost_to_follow_up_Other = obsutils.getVariableValueFromObs(reason_for_lost_to_follow_up_other_obs)


        record = {

            "uuid": demographicsutils.get_patient_demographics(doc).get("patientUuid"),
            "state": facility_info.get("State") if facility_info else None,
            "lga" : facility_info.get("LGA") if facility_info else None,
            "facility_name": header.get("facilityName"),
            "datim_code": header.get("facilityDatimCode"),
            "patient_id": demographicsutils.get_patient_identifier(4, doc),
            "hospital_no": demographicsutils.get_patient_identifier(5, doc),
            # "dob": birthdate,
            # "sex": demographics.get("gender"),
            # "art_start_date": art_start_date,
            "last_pickup_date": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
            # "days_of_arv_pickup": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
            # "pill_balance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),

            "Date of Tracking": last_ctd_encounter,
            "Reason for Tracking": reason_for_tracking,
            # "Guardian / Treatment Partner's Name": "",
            # "Guardian / Treatment Partner's Contact Address": "",
            # "Guardian / Treatment Partner's Phone Number": "",
            "Date of Last Actual Contact/ Appointment": Date_of_Last_Actual_Contact,
            "Date of Missed Scheduled Appointment": Date_of_Missed_Scheduled_Appointment,
            "Client Verification": Client_Verification,
            "Indication for Client Verification": Indication_for_Client_Verification,
            "Patient Care in Facility Discontinued": Patient_Care_in_Facility_Discontinued,
            "Date of Discontinuation": Date_of_Discontinuation,
            "Reason for Discontinuation": Reason_for_Discontinuation,
            "Facility transferred to": Facility_transferred_to,
            "Cause of Death": Cause_of_Death,
            "VA Cause of Death": VA_Cause_of_Death,
            "Adult Causes": Adult_Causes,
            "Child Causes": Child_Causes,
            "Other cause of death": Other_cause_of_death,
            "Reason to Discontinue Care": Reason_to_Discontinue_Care,
            "Discontinue Care other specify": Discontinue_Care_other_specify,
            "Date of Lost to follow up": Date_of_Lost_to_follow_up,
            "Reason for Lost to follow up": Reason_for_Lost_to_follow_up,
            "Reason for Lost to follow up_Other": Reason_for_Lost_to_follow_up_Other,

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

