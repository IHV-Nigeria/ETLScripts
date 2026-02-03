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



def export_eac_data(cutoff_datetime=None, filename=None ):
    db = mongo_dao.get_db_connection("cdr")
    cursor = mongo_dao.get_art_containers(db)
    size = mongo_dao.get_art_container_size(db)
    print(f"Processing {size} ART containers...")

    BATCH_SIZE = 1000
    batch_list = []

    # 1. Prepare the file path (create directory and name)
    full_path = prepare_filepath(filename)
    
    # Track if it's the first batch so we can write the CSV header
    is_first_batch = True
    
    #extracted_results = []
    for doc in tqdm(cursor, total=size, desc="EAC ETL Progress"):
            header = demographicsutils.get_message_header(doc)
            demographics = demographicsutils.get_patient_demographics(doc)
            birthdate = demographics.get("birthdate")
            art_start_date = artcommence.get_art_start_date(doc, cutoff_datetime)
            eac_1_date = eacutils.get_eac_date(1, doc)
            last_eac_encounter=eacutils.get_last_eac_encounter(doc,cutoff_datetime)
            viral_load_before_first_eac_obs = labutils.get_last_viral_load_obs_before(doc, eac_1_date)
            viral_load_1_obs = labutils.get_nth_viral_load_obs(doc, 1, cutoff_datetime)
            viral_load_2_obs = labutils.get_nth_viral_load_obs(doc, 2, cutoff_datetime)
            viral_load_3_obs = labutils.get_nth_viral_load_obs(doc, 3, cutoff_datetime)
            current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime)  
            last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime) 
            current_pregnancy_status_obs=carecardutils.get_current_pregnancy_status_obs(doc,cutoff_datetime)


            record = {
                "touchtime": header.get("touchTime"),
                "facilityState": header.get("facilityState"),
                "facilityLga" : header.get("facilityLga"),
                "facilityDatimCode" : header.get("facilityDatimCode"),
                "facilityName": header.get("facilityName"),
                "patientUniqueID": demographicsutils.get_patient_identifier(4, doc),
                "patientHospitalID": demographicsutils.get_patient_identifier(5, doc),

                "sex": demographics.get("gender"),
                "birthdate": demographics.get("birthdate"),
            
                "artStartDate": artcommence.get_art_start_date(doc, cutoff_datetime),
                "ageAtARTStartMonths": demographicsutils.get_pediatric_age_art_start_months(doc, birthdate, art_start_date),
                "ageAtARTStartYears": demographicsutils.get_age_art_start_years(doc, birthdate, art_start_date),
                "careEntryPoint": hivenrollmentutils.get_care_entry_point(doc,cutoff_datetime),
                "kpType": hivenrollmentutils.get_kp_type(doc,cutoff_datetime),
                "monthsOnArt": demographicsutils.get_months_on_art(doc,art_start_date,cutoff_datetime),
                "dateTransferredIn": hivenrollmentutils.get_date_transferred_in(doc,cutoff_datetime),
                "transferredInStatus": hivenrollmentutils.get_prior_art(doc,cutoff_datetime),
                "baselineWeight": carecardutils.get_first_weight(doc,cutoff_datetime),
                "lastPickupDate": pharmacyutils.get_last_arv_pickup_date(doc,cutoff_datetime),
                "lastVisitDate": encounterutils.get_last_encounter_date(doc,cutoff_datetime),
                "daysOfARVRefill": pharmacyutils.get_last_drug_pickup_duration(doc,last_arv_pickup_obs),
                "pillBalance": pharmacyutils.get_pill_balance(doc,last_arv_pickup_obs),
                "viralLoadBefore1stEAC": obsutils.getValueNumericFromObs(viral_load_before_first_eac_obs),
                "viralLoadBefore1stEACDate": obsutils.getObsDatetimeFromObs(viral_load_before_first_eac_obs),
                "viralLoadBefore1stEACSampleCollectionDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc,viral_load_before_first_eac_obs)),
                "viralLoadBefore1stEACReportedDate": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc,viral_load_before_first_eac_obs)),
                "eac1date": eacutils.get_eac_date(1, doc),
                "eac2date": eacutils.get_eac_date(2, doc),
                "eac3date": eacutils.get_eac_date(3, doc),
                "eac4date": eacutils.get_eac_date(4, doc),
                "eac5date": eacutils.get_eac_date(5, doc),
                "eac6date": eacutils.get_eac_date(6, doc),
                "eac7date": eacutils.get_eac_date(7, doc),
                "eac8date": eacutils.get_eac_date(8, doc),
                "viral_load_1": obsutils.getValueNumericFromObs(viral_load_1_obs),
                "viral_load_1_reported_date": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_1_obs)),
                "viral_load_1_sample_collection_date": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_1_obs)),
                "viral_load_2": obsutils.getValueNumericFromObs(viral_load_2_obs),
                "viral_load_2_reported_date": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_2_obs)),
                "viral_load_2_sample_collection_date": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_2_obs)),
                "viral_load_3": obsutils.getValueNumericFromObs(viral_load_3_obs),
                "viral_load_3_reported_date": obsutils.getValueDatetimeFromObs(labutils.get_reported_date_obs_of_viral_load_obs(doc, viral_load_3_obs)),
                "viral_load_3_sample_collection_date": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_3_obs)),
                
                "currentRegimenLine": pharmacyutils.get_current_regimen_line(doc,cutoff_datetime) ,
                "currentRegimen": pharmacyutils.get_current_regimen(doc,cutoff_datetime),
                "secondLineRegimenStartDate": pharmacyutils.get_min_second_line_regimen_date(doc,cutoff_datetime),
                "thirdLineRegimenStartDate": pharmacyutils.get_min_third_line_regimen_date(doc,cutoff_datetime),
                "currentPregnancyStatus": obsutils.getVariableValueFromObs(current_pregnancy_status_obs),
                "currentPregnancyStatusDatetime": obsutils.getObsDatetimeFromObs(current_pregnancy_status_obs),
                "edd": obsutils.getVariableValueFromObs(carecardutils.get_edd_for_last_pregnancy(doc,current_pregnancy_status_obs)),

                

                "lastEACSessionType": eacutils.get_last_eac_session_type(doc,last_eac_encounter,cutoff_datetime),
                "lastEACSessionDate": encounterutils.get_encounter_datetime (last_eac_encounter),
                "lastEACBarriersToAdherence": eacutils.get_last_eac_barriers_to_adherence(doc,last_eac_encounter, cutoff_datetime),
                "lastEACRegimenPlan": eacutils.get_last_eac_regimen_plan(doc, last_eac_encounter, cutoff_datetime),
                "lastEACFollowupDate": eacutils.get_last_eac_followup_date(doc, last_eac_encounter, cutoff_datetime),
                "lastEACAdherenceComments": eacutils.get_last_eac_comments(doc, last_eac_encounter, cutoff_datetime),
                "currentViralLoad": obsutils.getValueNumericFromObs(current_viral_load_obs),
                "viralLoadEncounterDate": obsutils.getObsDatetimeFromObs(current_viral_load_obs),
                "viralLoadSampleDate": obsutils.getValueDatetimeFromObs(labutils.get_sample_collection_date_obs_of_viral_load_obs(doc, current_viral_load_obs)),
                "currentViralLoadIndication": obsutils.getVariableValueFromObs(labutils.get_viral_load_indication_obs_of_viral_load_obs(doc, current_viral_load_obs)),
                "lastSampleTakenDate": obsutils.getValueDatetimeFromObs(labutils.get_last_sample_taken_date_obs(doc,cutoff_datetime)),
                "patientOutcome" : ctdutils.get_patient_outcome (doc,cutoff_datetime),
                "patientOutcomeDate" : ctdutils.get_outcome_date (doc,cutoff_datetime),
                "currentArtStatus": pharmacyutils.get_current_art_status(doc,cutoff_datetime),
                "dispensing_modality": pharmacyutils.get_last_dsd_model(doc,cutoff_datetime),
                "facility_dispensing_modality": pharmacyutils.get_facility_dsd_model(doc,cutoff_datetime),
                "ddd_dispensing_modelity": pharmacyutils.get_ddd_dsd_model(doc,cutoff_datetime),
                "mmd_status": pharmacyutils.get_mmd_type(doc,cutoff_datetime),
                "pharmacy_next_appointment_date": pharmacyutils.get_pharmacy_next_appointment_date(doc, cutoff_datetime),
                "clinical_next_appointment_date": carecardutils.get_clinical_next_appointment_date(doc,cutoff_datetime),
                "currentAge": demographicsutils.get_current_age_at_date(doc,cutoff_datetime),
                "currentAgeInMonths": demographicsutils.get_current_age_at_date_in_months(doc,cutoff_datetime),
                "patientUUID": demographicsutils.get_patient_demographics(doc).get("patientUuid"),
                "quater": commonutils.get_fy_and_quater_from_date(obsutils.getObsDatetimeFromObs(current_viral_load_obs)), # type: ignore
                
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



