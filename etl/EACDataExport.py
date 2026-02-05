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
    db_name="cdr"
    db = mongo_dao.get_db_connection(db_name)
    cursor = mongo_dao.get_art_containers(db,db_name)
    size = mongo_dao.get_art_container_size(db,db_name)
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
            birthdate = commonutils.validate_date(demographics.get("birthdate"))
            art_start_date = commonutils.validate_date(artcommence.get_art_start_date(doc, cutoff_datetime))
            eac_1_date = commonutils.validate_date(eacutils.get_eac_date(1, doc))
            last_eac_encounter=eacutils.get_last_eac_encounter(doc,cutoff_datetime)
            viral_load_before_first_eac_obs = labutils.get_last_viral_load_obs_before(doc, eac_1_date)
            viral_load_1_obs = labutils.get_nth_viral_load_obs(doc, 1, cutoff_datetime)
            viral_load_2_obs = labutils.get_nth_viral_load_obs(doc, 2, cutoff_datetime)
            viral_load_3_obs = labutils.get_nth_viral_load_obs(doc, 3, cutoff_datetime)
            current_viral_load_obs = labutils.get_last_viral_load_obs_before(doc, cutoff_datetime)  
            last_arv_pickup_obs = pharmacyutils.get_last_arv_obs(doc, cutoff_datetime) 
            current_pregnancy_status_obs=carecardutils.get_current_pregnancy_status_obs(doc,cutoff_datetime)


            record = {
                #"touchtime": header.get("touchTime"),
                "State": header.get("facilityState"),
                "LGA" : header.get("facilityLga"),
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
                "EAC1date": eacutils.get_eac_date(1, doc),
                "EAC2date": eacutils.get_eac_date(2, doc),
                "EAC3date": eacutils.get_eac_date(3, doc),
                "EAC4date": eacutils.get_eac_date(4, doc),
                "EAC5date": eacutils.get_eac_date(5, doc),
                "EAC6date": eacutils.get_eac_date(6, doc),
                "EAC7date": eacutils.get_eac_date(7, doc),
                "EAC8date": eacutils.get_eac_date(8, doc),
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
                
                #"kpType": hivenrollmentutils.get_kp_type(doc,cutoff_datetime),
                                         
                #"baselineWeight": carecardutils.get_first_weight(doc,cutoff_datetime),
                
                #"currentAgeInMonths": demographicsutils.get_current_age_at_date_in_months(doc,cutoff_datetime),
                
                
                
                
                
                
                
                
                
                
                
                
               
                
               
                

                

               
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
                
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



