#import mongo_utils as  utils
#import constants as constants
from email import utils
import pandas as pd
from tqdm import tqdm
from datetime import datetime, date
import os

import dao.mongodbdao as mongo_dao
import dao.postgresdao as postgres_dao
from utils import biometricutils
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



def export_art_line_list_data(cutoff_datetime=None):

    
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
    BATCH_SIZE = 500 # Increased for 700k records
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
                hivconfirmeddateobs=hivenrollmentutils.get_last_date_confirmed_hiv_positive_obs(doc,cutoff_datetime)
                hivconfirmeddate = obsutils.getValueDatetimeFromObs(hivconfirmeddateobs)
                last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)
                initial_cd4_count_obs=artcommence.get_cd4_count_obs(doc, cutoff_datetime)
                initial_cd4_count_value=obsutils.getValueNumericFromObs(initial_cd4_count_obs)
                initial_cd4_count_date = obsutils.getObsDatetimeFromObs(initial_cd4_count_obs)
                current_cd4_obs = labutils.get_current_cd4_count_obs(doc, cutoff_datetime)
                current_cd4_value = obsutils.getValueNumericFromObs(current_cd4_obs) if current_cd4_obs else None
                current_cd4_date = obsutils.getObsDatetimeFromObs(current_cd4_obs) if current_cd4_obs else None
                last_eac_encounter=eacutils.get_last_eac_encounter(doc,cutoff_datetime)
                pregnancy_status_obs= carecardutils.get_current_pregnancy_status_obs(doc,cutoff_datetime)
                pregnancy_status=obsutils.getVariableValueFromObs(pregnancy_status_obs) if pregnancy_status_obs else None
                pregnancy_status_date=obsutils.getObsDatetimeFromObs(pregnancy_status_obs) if pregnancy_status_obs else None
                edd_obs=carecardutils.get_edd_for_last_pregnancy(doc, pregnancy_status_obs) if pregnancy_status_obs else None
                edd_date=obsutils.getValueDatetimeFromObs(edd_obs) if edd_obs else None
                last_delivery_edd_obs=carecardutils.get_last_delivery_edd_obs(doc, cutoff_datetime)
                last_delivery_edd_date=obsutils.getValueDatetimeFromObs(last_delivery_edd_obs) if last_delivery_edd_obs else None
                lmp_obs=carecardutils.get_lmp_for_last_pregnancy(doc, pregnancy_status_obs) if pregnancy_status_obs else None
                lmp_date=obsutils.getValueDatetimeFromObs(lmp_obs) if lmp_obs else None
                gestation_weeks_obs=carecardutils.get_gestation_weeks_for_last_pregnancy_obs(doc, pregnancy_status_obs) if pregnancy_status_obs else None
                gestation_age_weeks=obsutils.getValueNumericFromObs(gestation_weeks_obs) if gestation_weeks_obs else None
                viral_load_obs=labutils.get_last_viral_load_obs_before(doc, cutoff_datetime)
                viral_load_value=obsutils.getValueNumericFromObs(viral_load_obs) if viral_load_obs else None
                viral_load_date=obsutils.getObsDatetimeFromObs(viral_load_obs) if viral_load_obs else None
                viral_load_sample_collection_obs=labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_obs) if viral_load_obs else None
                viral_load_sample_collection_date=obsutils.getValueDatetimeFromObs(viral_load_sample_collection_obs) if viral_load_sample_collection_obs else None
                viral_load_reported_date_obs=labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_obs) if viral_load_obs else None
                viral_load_reported_date=obsutils.getValueDatetimeFromObs(viral_load_reported_date_obs) if viral_load_reported_date_obs else None
                viral_load_result_date_obs=labutils.get_result_date_obs_of_viral_load_obs(doc, viral_load_obs) if viral_load_obs else None
                viral_load_result_date=obsutils.getValueDatetimeFromObs(viral_load_result_date_obs) if viral_load_result_date_obs else None
                viral_load_assay_date_obs=labutils.get_assay_date_obs_of_viral_load_obs(doc, viral_load_obs) if viral_load_obs else None
                viral_load_assay_date=obsutils.getValueDatetimeFromObs(viral_load_assay_date_obs) if viral_load_assay_date_obs else None
                viral_load_approval_date_obs=labutils.get_approval_date_obs_of_viral_load_obs(doc, viral_load_obs) if viral_load_obs else None
                viral_load_approval_date=obsutils.getValueDatetimeFromObs(viral_load_approval_date_obs) if viral_load_approval_date_obs else None
                viral_load_indication_obs=labutils.get_viral_load_indication_obs_of_viral_load_obs(doc, viral_load_obs) if viral_load_obs else None
                viral_load_indication=obsutils.getVariableValueFromObs(viral_load_indication_obs) if viral_load_indication_obs else None
                weight_obs=carecardutils.get_current_weight_obs(doc,cutoff_datetime)
                weight_kg=obsutils.getValueNumericFromObs(weight_obs) if weight_obs else None
                weight_date=obsutils.getObsDatetimeFromObs(weight_obs) if weight_obs else None
                tb_status_obs=carecardutils.get_current_tb_status_obs(doc,cutoff_datetime) 
                tb_status=obsutils.getVariableValueFromObs(tb_status_obs) if tb_status_obs else None


                record = {
                    "touchtime": header.get("touchTime"),
                    "patientuuid": demographics.get("patientUuid"),
                    "cuttoffperiod": cutoff_datetime,
                    "state": facility_info.get("State") if facility_info else None,
                    "lga" : facility_info.get("LGA") if facility_info else None,
                    "datimcode" : header.get("facilityDatimCode"),
                    "facilityname": header.get("facilityName"),
                    "patientuniqueid": demographicsutils.get_patient_identifier(4, doc),
                    "patienthospitalno": demographicsutils.get_patient_identifier(5, doc),
                    "ancnoidentifier": "", # Placeholder for ANC number if needed in the future
                    "ancnoconceptid": "", # Placeholder for ANC number concept ID if needed in the future
                    "htsno":"", # Placeholder for HTS number if needed in the future
                    "sex": demographics.get("gender"),
                    "ageatstartofartyears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
                    "ageatstartofartmonths": demographicsutils.get_pediatric_age_art_start_months(doc, birthdate, art_start_date),
                    "careentrypoint": hivenrollmentutils.get_care_entry_point(doc,cutoff_datetime),
                    "hivconfirmeddate": hivconfirmeddate,
                    "monthsonart": demographicsutils.get_months_on_art(doc, art_start_date, cutoff_datetime),
                    "datetransferredin": hivenrollmentutils.get_date_transferred_in(doc,cutoff_datetime),
                    "transferinstatus": hivenrollmentutils.get_prior_art(doc,cutoff_datetime),
                    "artstartdate": art_start_date,
                    "lastpickupdate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
                    "lastvisitdate": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
                    "daysofarvrefil": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
                    "pillbalance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),
                    "initialregimenline": artcommence.get_current_regimen_line(doc,cutoff_datetime),
                    "initialregimen": artcommence.get_current_regimen(doc,cutoff_datetime),
                    "initialcd4count": initial_cd4_count_value,
                    "initialcd4countdate": initial_cd4_count_date,
                    "currentcd4count": current_cd4_value,
                    "currentcd4countdate": current_cd4_date,
                    "lasteacdate": encounterutils.get_encounter_datetime (last_eac_encounter),
                    "currentregimenline": pharmacyutils.get_current_regimen_line(doc, cutoff_datetime),
                    "currentregimen": pharmacyutils.get_current_regimen(doc, cutoff_datetime),
                    "pregnancystatus": pregnancy_status,
                    "pregnancystatusdate": pregnancy_status_date,
                    "edd": edd_date,
                    "lastdeliverydate": last_delivery_edd_date,
                    "lmp": lmp_date,
                    "gestationageweeks": gestation_age_weeks,
                    "currentviralload": viral_load_value,
                    "viralloadencounterdate": viral_load_date,
                    "viralloadsamplecollectiondate": viral_load_sample_collection_date,
                    "viralloadreporteddate": viral_load_reported_date,
                    "resultdate": viral_load_result_date,
                    "assaydate": viral_load_assay_date,
                    "approvaldate": viral_load_approval_date, 
                    "viralloadindication": viral_load_indication,
                    "patientoutcome": ctdutils.get_patient_outcome (doc,cutoff_datetime),
                    "patientoutcomedate":  ctdutils.get_outcome_date (doc,cutoff_datetime),
                    "currentartstatus": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
                    "dispensingmodality": pharmacyutils.get_last_dsd_model(doc,cutoff_datetime),
                    "facilitydispensingmodality": pharmacyutils.get_facility_dsd_model(doc,cutoff_datetime),
                    "ddddispensingmodality": pharmacyutils.get_ddd_dsd_model(doc,cutoff_datetime),
                    "mmdtype": pharmacyutils.get_mmd_type(doc,cutoff_datetime),
                    "datereturnedtocare": ctdutils.get_date_returned_to_care(doc,cutoff_datetime),
                    "dateoftermination": ctdutils.get_date_of_termination(doc,cutoff_datetime),
                    "pharmacynextappointment": pharmacyutils.get_pharmacy_next_appointment_date(doc,cutoff_datetime),
                    "clinicalnextappointment": carecardutils.get_clinical_next_appointment_date(doc,cutoff_datetime),
                    "currentageyears": demographicsutils.get_current_age_at_date(doc,cutoff_datetime),
                    "currentagemonths": demographicsutils.get_current_age_at_date_in_months(doc,cutoff_datetime),""
                    "dateofbirth": birthdate,
                    "markasdeseased": False,
                    "markasdeseaseddeathdate": None, # Placeholder for any additional fields that may be added in the future
                    "registrationphoneno": "", # Placeholder for registration phone number if needed in the future
                    "nextofkinphoneno": "", # Placeholder for next of kin phone number if needed in the future
                    "treatmentsupporterphoneno": "", # Placeholder for treatment supporter phone number if needed in the future
                    "biometriccaptured": "Yes" if biometricutils.has_biometric_captured(doc) else "No", # Yes or No based on whether biometric data exists for the patient
                    "biometriccapturedate": biometricutils.get_biometric_capture_date(doc), # Date when biometric data was captured, if available
                    "validcapture": "Yes", # Put yes by default. The new_template column is missing in the CDR
                    "currentweight_kg": weight_kg,
                    "currentweightdate": weight_date,
                    "tbstatus": tb_status



















                
        
                }
                batch_list.append(record)

                if len(batch_list) >= BATCH_SIZE:
                    postgres_dao.save_to_postgres(conn, "art_line_list", batch_list)
                    total_inserted += len(batch_list)
                    batch_list.clear() # clear() is slightly more memory efficient than []
                
   
         # Final Batch
        if batch_list:
            postgres_dao.save_to_postgres(conn, "art_line_list", batch_list)
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




