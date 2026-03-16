#import mongo_utils as  utils
#import constants as constants
from email import utils
import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
import os

import dao.mongodbdao as mongo_dao
import dao.postgresdao as postgres_dao
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

# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}



def export_ctd_line_list_data(cutoff_datetime=None):

    db_name=MONGO_DATABASE_NAME
    db = mongo_dao.get_db_connection(db_name)
    cursor = mongo_dao.get_art_containers(db,db_name)
    size = mongo_dao.get_art_container_size(db,db_name)
    conn=postgres_dao.connect_to_postgresqldb()
    if conn is None:
        print("Failed to connect to PostgreSQL. Data not saved.")
        return
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)
    BATCH_SIZE = 500
    batch_list = []
    total_inserted = 0


    try:
        #extracted_results = []
        for doc in tqdm(cursor, total=size, desc="ART Line List ETL Progress"):


            if not is_aspire_state(doc):
                continue  # Skip this record and move to the next one

            header = demographicsutils.get_message_header(doc)
            datim_code = header.get("facilityDatimCode")
            demographics = demographicsutils.get_patient_demographics(doc)
            birthdate = commonutils.validate_date(demographics.get("birthdate"))
            facility_info = get_facility_by_datim(datim_code)
            art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
            last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)
            last_sample_collection_obs = labutils.get_last_sample_taken_date_obs(doc,cutoff_datetime)
            last_sample_collection_date = last_sample_collection_obs.get("ObsDateTime") if last_sample_collection_obs else None
            last_viral_load_obs = labutils.get_last_viral_load_obs_before(doc,cutoff_datetime)
            last_viral_load = last_viral_load_obs.get("variableValue") if last_viral_load_obs else None




            record = {

                "id": demographics.get("patientUuid"),
                "cuttoffperiod": cutoff_datetime,
                "create_date": commonutils.format_date(demographics.get("dateCreated")),
                "update_date": commonutils.format_date(header.get("touchTime")),
                "state": facility_info.get("State") if facility_info else None,
                "lga" : facility_info.get("LGA") if facility_info else None,
                "facility_name": header.get("facilityName"),
                "datim_code": header.get("facilityDatimCode"),
                "patient_id": demographicsutils.get_patient_identifier(4, doc),
                "hospital_no": demographicsutils.get_patient_identifier(5, doc),
                "dob": birthdate,
                "current_age": demographicsutils.calculateAge(birthdate),
                "sex": demographics.get("gender"),
                "current_art_status": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
                "last_visit_date": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
                "art_start_date": art_start_date,
                "last_pickup_date": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
                "days_of_arv_pickup": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
                "pill_balance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),
                "viral_load_sample_collection_date": last_sample_collection_date,
                "last_viral_load": last_viral_load,
            }
            batch_list.append(record)

            if len(batch_list) >= BATCH_SIZE:
                postgres_dao.save_to_postgres(conn, "ctd_line_list", batch_list)
                total_inserted += len(batch_list)
                batch_list.clear() # clear() is slightly more memory efficient than []


        # Final Batch
        if batch_list:
            postgres_dao.save_to_postgres(conn, "ctd_line_list", batch_list)
            total_inserted += len(batch_list)

    except Exception as e:
        print(f"Critical error during ETL: {e}")
        conn.rollback()
    finally:
        # ALWAYS close connections
        conn.close()
        # db.client.close() # Depending on your mongo_dao implementation
        print(f"\nETL Complete. Records Skipped: {size - total_inserted}. Records Inserted: {total_inserted}")

    print(f"\nBatch insert to postgresql completed. Total records processed: {size}")




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
