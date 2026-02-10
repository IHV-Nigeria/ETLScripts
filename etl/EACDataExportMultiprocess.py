#import mongo_utils as  utils
#import constants as constants
from email import utils
import multiprocessing
import queue
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


# Global cache to store facilities for O(1) lookup speed
_facility_cache = {}

def producer_consumer_etl(num_consumers=4, cutoff_datetime=None, filename="EAC_Export.csv"):
    
     # Producer logic (same as the original export_eac_data function but instead of processing, it puts documents in the queue)
    db_name="cdr"
    db = mongo_dao.get_db_connection(db_name)
    cursor = mongo_dao.get_art_containers(db,db_name)
    size = mongo_dao.get_art_container_size(db,db_name)
    print(f"Processing {size} ART containers...")
    load_facility_cache(db, db_name)

    # Create a multiprocessing queue
    work_queue = multiprocessing.JoinableQueue(maxsize=4000) # Limit size to save RAM

    # Start consumer processes
    consumers = []
    for i in range(num_consumers):
        p = multiprocessing.Process(
            target=consumer, 
            args=(work_queue, i, cutoff_datetime, filename)
        )
        p.daemon = True
        p.start()
        consumers.append(p)

   

    for doc in tqdm(cursor, total=size, desc="EAC ETL Progress"):
        work_queue.put(doc) # Pushes document into the queue

    # Signal consumers to stop by putting sentinel values (None)
    for _ in range(num_consumers):
        work_queue.put(None)

    # Wait for all tasks to be done
    work_queue.join() # Wait for all tasks to be finished

    print("ETL successfully completed across all processes.") 

def consumer(queue, process_id, cutoff_datetime, filename):
    proc_filename = f"proc_{process_id}_{filename}"
    proc_filename=prepare_filepath(proc_filename)

    db = mongo_dao.get_db_connection("cdr")
    load_facility_cache(db, "cdr")  # Ensure each process has the facility cache loaded


    while True:
        doc = queue.get()
        if doc is None:  # Sentinel value to signal completion
            queue.task_done()
            break
        if not is_aspire_state(doc):
            queue.task_done()
            continue  # Skip this record and move to the next one
        try:
             # Process the document (same logic as in the original loop)
             # You can refactor the document processing logic into a separate function for cleaner code
            record=process_document(doc, cutoff_datetime)
                # Save the record to a CSV file (you can batch this for better performance)
            save_batch_to_csv([record], proc_filename, write_header=not os.path.exists(proc_filename))

        except Exception as e:
            print(f"Error processing document in process {process_id}: {e}")
        finally:
            queue.task_done()
       
        
    print(f"Process {process_id} completed.")

def process_document(doc, cutoff_datetime):
    # This function contains the logic to extract data from a single document
    # and return a dictionary of the extracted values.
    # You can take the code inside the original for loop and place it here,
    # making sure to return the 'record' dictionary at the end.
    record = {}
    header = demographicsutils.get_message_header(doc)
    demographics = demographicsutils.get_patient_demographics(doc)
    birthdate = commonutils.validate_date(demographics.get("birthdate"))
    datim_code = header.get("facilityDatimCode")
    facility_info = get_facility_by_datim(datim_code)
    art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
    eac_1_date = commonutils.validate_date(eacutils.get_eac_date(1, doc))
    last_eac_encounter = eacutils.get_last_eac_encounter(doc, cutoff_datetime)
    viral_load_before_first_eac_obs = labutils.get_last_viral_load_obs_before(doc, eac_1_date)
    viral_load_1_obs = labutils.get_nth_viral_load_obs(doc, 1, cutoff_datetime)
    viral_load_2_obs = labutils.get_nth_viral_load_obs(doc, 2, cutoff_datetime)
    viral_load_3_obs = labutils.get_nth_viral_load_obs(doc, 3, cutoff_datetime)
    current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime) 
    current_viral_load_obsdatetime = obsutils.getObsDatetimeFromObs(current_viral_load_obs) if current_viral_load_obs else None 
    last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime) 
    current_pregnancy_status_obs = carecardutils.get_current_pregnancy_status_obs(doc, cutoff_datetime)


    record = {
                #"touchtime": header.get("touchTime"),
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
                "LastPickupDate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
                "LastVisitDate": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
                "DaysOfARVRefill": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
                "PillBalance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),
                "PatientOutcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime),
                "PatientOutcomeDate" : ctdutils.get_outcome_date (doc,cutoff_datetime),
                "CurrentArtStatus": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
                "DispensingModality": pharmacyutils.get_last_dsd_model(doc,cutoff_datetime),
                "FacilityDispensingModality": pharmacyutils.get_facility_dsd_model(doc,cutoff_datetime),
                "DDDDispensingModality": pharmacyutils.get_ddd_dsd_model(doc,cutoff_datetime),
                "MMDType": pharmacyutils.get_mmd_type(doc,cutoff_datetime),
                "PharmacyNextAppointmentDate": pharmacyutils.get_pharmacy_next_appointment_date(doc, cutoff_datetime),
                "ClinicalNextAppointmentDate": carecardutils.get_clinical_next_appointment_date(doc,cutoff_datetime),
                "CurrentViralLoad": obsutils.getValueNumericFromObs(current_viral_load_obs),
                "ViralLoadEncounterDate": obsutils.getObsDatetimeFromObs(current_viral_load_obs),
                "ViralLoadSampleDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, current_viral_load_obs)),
                "ViralLoadIndication": obsutils.getVariableValueFromObs(labutils.get_viral_load_indication_obs_of_viral_load_obs(doc, current_viral_load_obs)),
                "LastSampleTakenDate": obsutils.getValueDatetimeFromObs(labutils.get_last_sample_taken_date_obs(doc,cutoff_datetime)),
                "ViralLoadBefore1stEAC": obsutils.getValueNumericFromObs(viral_load_before_first_eac_obs),
                "ViralLoadBefore1stEACDate": obsutils.getObsDatetimeFromObs(viral_load_before_first_eac_obs),
                "ViralLoadBefore1stEACSampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc,viral_load_before_first_eac_obs)),
                "ViralLoadBefore1stEACReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc,viral_load_before_first_eac_obs)),
                "EAC1date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,1,current_viral_load_obsdatetime)),
                "EAC2date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,2,current_viral_load_obsdatetime)),
                "EAC3date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,3,current_viral_load_obsdatetime)),
                "EAC4date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,4,current_viral_load_obsdatetime)),
                "EAC5date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,5,current_viral_load_obsdatetime)),
                "EAC6date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,6,current_viral_load_obsdatetime)),
                "EAC7date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,7,current_viral_load_obsdatetime)),
                "EAC8date": encounterutils.get_encounter_datetime(eacutils.get_nth_eac_after_date(doc,8,current_viral_load_obsdatetime)),
                "ViralLoad1": obsutils.getValueNumericFromObs(viral_load_1_obs),
                "ViralLoad1ReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_1_obs)),
                "ViralLoad1SampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_1_obs)),
                "ViralLoad2": obsutils.getValueNumericFromObs(viral_load_2_obs),
                "ViralLoad2ReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_2_obs)),
                "ViralLoad2SampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_2_obs)),
                "ViralLoad3": obsutils.getValueNumericFromObs(viral_load_3_obs),
                "ViralLoad3ReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_3_obs)),
                "ViralLoad3SampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_3_obs)),
                "CurrentRegimenLine": pharmacyutils.get_current_regimen_line(doc,cutoff_datetime) ,
                "CurrentRegimen": pharmacyutils.get_current_regimen(doc,cutoff_datetime),
                "SecondLineRegimenStartDate": pharmacyutils.get_min_second_line_regimen_date(doc,cutoff_datetime),
                "ThirdLineRegimenStartDate": pharmacyutils.get_min_third_line_regimen_date(doc,cutoff_datetime),
                "CurrentPregnancyStatus": obsutils.getVariableValueFromObs(current_pregnancy_status_obs),
                "CurrentPregnancyStatusDatetime": obsutils.getObsDatetimeFromObs(current_pregnancy_status_obs),
                "EDD": obsutils.getValueDatetimeFromObs(carecardutils.get_edd_for_last_pregnancy(doc,current_pregnancy_status_obs)),
                "LastEACSessionType": eacutils.get_last_eac_session_type(doc,last_eac_encounter,cutoff_datetime),
                "LastEACSessionDate": encounterutils.get_encounter_datetime (last_eac_encounter),
                "LastEACBarriersToAdherence": eacutils.get_last_eac_barriers_to_adherence(doc,last_eac_encounter, cutoff_datetime),
                "LastEACRegimenPlan": eacutils.get_last_eac_regimen_plan(doc, last_eac_encounter, cutoff_datetime),
                "LastEACFollowupDate": eacutils.get_last_eac_followup_date(doc, last_eac_encounter, cutoff_datetime),
                "LastEACAdherenceComments": eacutils.get_last_eac_comments(doc, last_eac_encounter, cutoff_datetime),
                "LastEACReferral": eacutils.get_eac_referral(doc, last_eac_encounter, cutoff_datetime),
                "LastReferralSwitchCommitteeDate": eacutils.get_referral_switch_commitee_date(doc, last_eac_encounter, cutoff_datetime),
                "PatientUUID": demographicsutils.get_patient_demographics(doc).get("patientUuid"),
                "Quater": commonutils.get_fy_and_quater_from_date(obsutils.getObsDatetimeFromObs(current_viral_load_obs)), # type: ignore
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
