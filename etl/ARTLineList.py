from tqdm import tqdm
from datetime import datetime, date
import dao.mongodbdao as mongo_dao
import dao.postgresdao as postgres_dao
from formslib import iptutils, otzutils
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
import logging


# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}


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



def _prefilter_stale_docs_before_conversion(doc_batch, conn, cutoff_datetime):
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
        return [], {"stale": 0, "invalid": invalid_key_count}

    key_pairs = [(item[1], item[2]) for item in keyed_docs]
    existing_touchtime_map = postgres_dao.get_art_line_list_existing_touchtimes(conn, key_pairs)

    stale_count = 0
    records = []
    for doc, patientuuid, datimcode, incoming_touchtime in keyed_docs:
        existing_touchtime = _to_naive_datetime(existing_touchtime_map.get((patientuuid, datimcode)))

        if existing_touchtime is not None and incoming_touchtime is not None and incoming_touchtime <= existing_touchtime:
            stale_count += 1
            continue

        records.append(convert_doc_to_record(doc, cutoff_datetime))

    return records, {"stale": stale_count, "invalid": invalid_key_count}



def upsert_art_line_list_data(cutoff_datetime=None):
    logging.basicConfig(filename='etl_errors.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

    db_name=MONGO_DATABASE_NAME
    #datims = ["LmLBtmd8U43"]
    db = mongo_dao.get_db_connection(db_name)
    cursor = mongo_dao.get_art_containers(db,db_name)
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
    batch_list = []
    doc_batch = []
    inserted_count = 0
    updated_count = 0
    skipped_count = 0
    prefilter_stale_count = 0
    prefilter_invalid_key_count = 0
    error_count = 0

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    previous_quarter_end_date = commonutils.get_previous_quarter_end_date(cutoff_datetime)

    
    try:
        #extracted_results = []
        for doc in tqdm(cursor, total=size, desc="ART Line List ETL Progress"):
            try:
                if not is_aspire_state(doc):
                    continue  # Skip this record and move to the next one

                doc_batch.append(doc)

                if len(doc_batch) >= BATCH_SIZE:
                    batch_list, prefilter_result = _prefilter_stale_docs_before_conversion(doc_batch, conn, cutoff_datetime)
                    prefilter_stale_count += prefilter_result.get('stale', 0)
                    prefilter_invalid_key_count += prefilter_result.get('invalid', 0)

                    if not batch_list:
                        doc_batch.clear()
                        continue

                    #postgres_dao.save_to_postgres(conn, "art_line_list", batch_list)
                    result_arr= postgres_dao.batch_upsert_art_line_list(conn, batch_list)
                    inserted_count += result_arr.get('inserted', 0)
                    updated_count += result_arr.get('updated', 0)
                    skipped_count += result_arr.get('skipped', 0)
                    batch_list.clear() # clear() is slightly more memory efficient than []
                    doc_batch.clear()


                 # Final Batch
                if doc_batch:
                    batch_list, prefilter_result = _prefilter_stale_docs_before_conversion(doc_batch, conn, cutoff_datetime)
                    prefilter_stale_count += prefilter_result.get('stale', 0)
                    prefilter_invalid_key_count += prefilter_result.get('invalid', 0)

                if batch_list:
                    #postgres_dao.save_to_postgres(conn, "art_line_list", batch_list)
                    result_arr = postgres_dao.batch_upsert_art_line_list(conn, batch_list)
                    inserted_count += result_arr.get('inserted', 0)
                    updated_count += result_arr.get('updated', 0)
                    skipped_count += result_arr.get('skipped', 0)
            except Exception as e:
                logging.error(f"Error processing batch: {e}. Batch details: {batch_list}")
                error_count += 1
                continue
                # Continue to next batch without rollback
    except Exception as e:
        logging.error(f"Error processing batch: {e}. Batch details: {batch_list}")

    finally:
        # ALWAYS close connections
        conn.close()
        # db.client.close() # Depending on your mongo_dao implementation
        total_skipped = skipped_count + prefilter_stale_count + prefilter_invalid_key_count
        print(f"\nETL Complete. Records Skipped: {total_skipped}. Records Inserted: {inserted_count}. Records Updated: {updated_count}")
        print(f"Prefiltered stale docs: {prefilter_stale_count}. Prefiltered invalid keys: {prefilter_invalid_key_count}. Upsert-level skips: {skipped_count}")
        print(f"Total batch errors during processing: {error_count}")
  
    print(f"\nBatch insert to postgresql completed. Total records processed: {size}")



def initialize_art_line_list_data(cutoff_datetime=None):

    
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
    BATCH_SIZE = 2000 # Increased for 700k records
    batch_list = []
    total_inserted = 0

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    previous_quarter_end_date = commonutils.get_previous_quarter_end_date(cutoff_datetime)

    
    try:
        #extracted_results = []
        for doc in tqdm(cursor, total=size, desc="ART Line List ETL Progress"):
            
           
                if not is_aspire_state(doc):
                    continue  # Skip this record and move to the next one

                record = convert_doc_to_record(doc, cutoff_datetime)
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

    previous_quarter_end_date = commonutils.get_previous_quarter_end_date(cutoff_datetime)
    header = demographicsutils.get_message_header(doc)
    datim_code = header.get("facilityDatimCode")
    demographics = demographicsutils.get_patient_demographics(doc)
    birthdate = commonutils.normalize_clinical_date(demographics.get("birthdate"))
    facility_info = get_facility_by_datim(datim_code)
    art_start_date = commonutils.normalize_clinical_date(artcommence.get_art_start_date(doc, cutoff_datetime))
    hivconfirmeddateobs=hivenrollmentutils.get_last_date_confirmed_hiv_positive_obs(doc,cutoff_datetime)
    hivconfirmeddate = obsutils.getValueDatetimeFromObs(hivconfirmeddateobs)
    #last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)
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
    tb_status_date=obsutils.getObsDatetimeFromObs(tb_status_obs) if tb_status_obs else None            
    last_inh_pickup_obs=pharmacyutils.get_last_isoniazid_prophylaxis_pickup_obs(doc,cutoff_datetime)
    last_inh_pickup_date=obsutils.getObsDatetimeFromObs(last_inh_pickup_obs)
    otz_enrollment_date = otzutils.get_otz_enrollment_date(doc, cutoff_datetime)
    initial_first_line_obs=pharmacyutils.get_min_first_line_regimen_obs(doc,cutoff_datetime)
    initial_second_line_obs=pharmacyutils.get_min_second_line_regimen_obs(doc,cutoff_datetime)
    initial_first_line_regimen=obsutils.getVariableValueFromObs(initial_first_line_obs) if initial_first_line_obs else None
    initial_second_line_regimen=obsutils.getVariableValueFromObs(initial_second_line_obs) if initial_second_line_obs else None 
    initial_first_line_regimen_date=obsutils.getObsDatetimeFromObs(initial_first_line_obs) if initial_first_line_obs else None
    initial_second_line_regimen_date=obsutils.getObsDatetimeFromObs(initial_second_line_obs) if initial_second_line_obs else None
    last_arv_pickup_obs=pharmacyutils.get_last_arv_obs(doc, cutoff_datetime)
    last_arv_pickup_date=obsutils.getObsDatetimeFromObs(last_arv_pickup_obs) if last_arv_pickup_obs else None
    last_arv_pickup_obsid=obsutils.getObsIDFromObs(last_arv_pickup_obs) if last_arv_pickup_obs else None
    arv_duration_days=pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs) if last_arv_pickup_obs else None
    pill_balance=pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs) if last_arv_pickup_obs else None
    initial_first_line_obs=pharmacyutils.get_initial_first_line_regimen_obs(doc,cutoff_datetime)
    initial_first_line_regimen_datex=obsutils.getObsDatetimeFromObs(initial_first_line_obs) if initial_first_line_obs else None
    last_arv_pickup_obs_previous_quarter=pharmacyutils.get_last_arv_obs(doc, previous_quarter_end_date)
    last_arv_pickup_date_previous_quarter=obsutils.getObsDatetimeFromObs(last_arv_pickup_obs_previous_quarter) if last_arv_pickup_obs_previous_quarter else None
    arv_duration_days_previous_quarter=pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs_previous_quarter) if last_arv_pickup_obs_previous_quarter else None
    patient_outcome_previous_quarter=ctdutils.get_patient_outcome (doc,previous_quarter_end_date)
    patient_outcome_previous_quarter_date=ctdutils.get_outcome_date (doc,previous_quarter_end_date)
    art_status_previous_quarter=pharmacyutils.get_current_art_status(doc, previous_quarter_end_date)
    arv_quantity_dispensed_previous_quarter=pharmacyutils.get_medication_quantity_dispensed(doc, last_arv_pickup_obs_previous_quarter) if last_arv_pickup_obs_previous_quarter else None
    arv_frequency_dispensed_previous_quarter=pharmacyutils.get_medication_frequency(doc, last_arv_pickup_obs_previous_quarter) if last_arv_pickup_obs_previous_quarter else None
    pill_balance_previous_quarter=pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs_previous_quarter) if last_arv_pickup_obs_previous_quarter else None
    cervical_cancer_screening_status_obs=carecardutils.get_cervical_cancer_screening_status_obs(doc, cutoff_datetime)
    cervical_cancer_screening_status_value=obsutils.getVariableValueFromObs(cervical_cancer_screening_status_obs) if cervical_cancer_screening_status_obs else None
    cervical_cancer_screening_status_date=obsutils.getObsDatetimeFromObs(cervical_cancer_screening_status_obs) if cervical_cancer_screening_status_obs else None
    cervical_cancer_treatment_provided_obs=carecardutils.get_cervical_cancer_treatment_provided_obs(doc, cervical_cancer_screening_status_obs.get("encounterId") if cervical_cancer_screening_status_obs else None)
    cervical_cancer_treatment_provided_value=obsutils.getVariableValueFromObs(cervical_cancer_treatment_provided_obs) if cervical_cancer_treatment_provided_obs else None
    cervical_cancer_treatment_provided_date=obsutils.getObsDatetimeFromObs(cervical_cancer_treatment_provided_obs) if cervical_cancer_treatment_provided_obs else None

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
               "sex": str(demographics.get("gender")) if demographics.get("gender") else None,
               "ageatstartofartyears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
               "ageatstartofartmonths": demographicsutils.get_pediatric_age_art_start_months(doc, birthdate, art_start_date),
               "careentrypoint": hivenrollmentutils.get_care_entry_point(doc,cutoff_datetime),
               "hivconfirmeddate": hivconfirmeddate,
               "monthsonart": demographicsutils.get_months_on_art(doc, art_start_date, cutoff_datetime),
               "datetransferredin": hivenrollmentutils.get_date_transferred_in(doc,cutoff_datetime),
               "transferinstatus": hivenrollmentutils.get_prior_art(doc,cutoff_datetime),
               "artstartdate": art_start_date,
               "lastpickupdate": last_arv_pickup_date,
               "lastvisitdate": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
               "daysofarvrefil": int(round(float(arv_duration_days))) if arv_duration_days else None,
               "pillbalance": int(round(float(pill_balance))) if pill_balance else None,
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
               "currentagemonths": demographicsutils.get_current_age_at_date_in_months(doc,cutoff_datetime),
                "dateofbirth": birthdate,
                "markasdeseased": False,
                "markasdeseaseddeathdate": None, # Placeholder for any additional fields that may be added in the future
                "registrationphoneno": "", # Placeholder for registration phone number if needed in the future
                "nextofkinphoneno": "", # Placeholder for next of kin phone number if needed in the future
                "treatmentsupporterphoneno": "", # Placeholder for treatment supporter phone number if needed in the future
                "biometriccaptured": "Yes" if biometricutils.has_biometric_captured(doc) else "No", # Yes or No based on whether biometric data exists for the patient
                "biometriccapturedate": biometricutils.get_biometric_capture_date(doc), # Date when biometric data was captured, if available
                "validcapture": "Yes" if biometricutils.has_biometric_captured(doc) else "", # Put yes by default. The new_template column is missing in the CDR
                "currentweight_kg": weight_kg,
                "currentweightdate": weight_date,
                "tbstatus": tb_status,
                "tbstatusdate": tb_status_date,
                "baselineinhstartdate": artcommence.get_baseline_inh_start_date(doc, cutoff_datetime),
                "baselineinhstopdate": artcommence.get_baseline_inh_stop_date(doc, cutoff_datetime),
                "currentinhstartdate": iptutils.get_inh_start_date(doc, cutoff_datetime),
                "currentinhoutcome": iptutils.get_inh_outcome(doc, cutoff_datetime),
                "currentinhoutcomedate": iptutils.get_inh_outcome_date(doc, cutoff_datetime),
                "lastinhdispenseddate": last_inh_pickup_date,
                "baselinetbtreatmentstartdate": artcommence.get_baseline_tb_treatment_start_date(doc,cutoff_datetime),
                "baselinetbtreatmentstopdate": artcommence.get_baseline_tb_treatment_stop_date(doc,cutoff_datetime),
                "lastviralloadsamplecollectionformdate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_obs)),
                "lastsampletakendate": obsutils.getValueDatetimeFromObs(labutils.get_last_sample_taken_date_obs(doc,cutoff_datetime)),
                "otzenrollmentdate": otz_enrollment_date,
                "otzoutcomedate": otzutils.get_otz_outcome_date(doc, cutoff_datetime),
                "enrollmentdate": demographicsutils.get_hiv_enrollment_date(doc),
                "initialfirstlineregimen": initial_first_line_regimen,
                "initialfirstlineregimendate": initial_first_line_regimen_datex,
                "initialsecondlineregimen": initial_second_line_regimen,
                "initialsecondlineregimendate": initial_second_line_regimen_date,
                "lastpickupdatepreviousquarter":  last_arv_pickup_date_previous_quarter, # Placeholder for last pickup date in the previous quarter if needed in the future
                "drugdurationpreviousquarter": arv_duration_days_previous_quarter, # Placeholder for drug duration in the previous quarter if needed in the future
                "patientoutcomepreviousquarter":  patient_outcome_previous_quarter, # Placeholder for patient outcome in the previous quarter if needed in the future
                "patientoutcomedatepreviousquarter": patient_outcome_previous_quarter_date, # Placeholder for patient outcome date in the previous quarter if needed in the future
                "artstatuspreviousquarter": art_status_previous_quarter, # Placeholder for ART status in the previous quarter if needed in the future
                "quantityofarvdispensedlastvisit": arv_quantity_dispensed_previous_quarter, # Placeholder for quantity of ARVs dispensed at last visit if needed in the future
                "frequencyofarvdispensedlastvisit": arv_frequency_dispensed_previous_quarter, # Placeholder for frequency of ARVs dispensed at last visit if needed in the future
                "currentartstatuswithpillbalance": None, # Placeholder for current ART status with pill balance if needed in the future
                "recapturedate": biometricutils.get_biometric_recapture_date(doc), # Placeholder for recapture date if needed in the future
                "recapturecount": biometricutils.get_biometric_recapture_count(doc), # Placeholder for recapture count if needed in the future
                "cervicalcancerscreeningstatus": cervical_cancer_screening_status_value , # Placeholder for cervical cancer screening status if needed in the future
                "cervicalcancerscreeningstatusdate": cervical_cancer_screening_status_date, # Placeholder for cervical cancer screening date if needed in the future
                "cervicalcancertreatmentprovided": cervical_cancer_treatment_provided_value, # Placeholder for cervical cancer treatment provided if needed in the future
                "cervicalcancertreatmentprovideddate": cervical_cancer_treatment_provided_date, # Placeholder for cervical cancer treatment provided date if needed in the future


            }
    return record





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




