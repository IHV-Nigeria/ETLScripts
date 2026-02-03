from typing import Optional
from pymongo import MongoClient
from pprint import pprint
from datetime import datetime, date
import pandas as pd
import constants as constants

def get_db_connection(db_name="cdr"):
    """Established connection to MongoDB and returns the database object."""
    client = MongoClient("mongodb://localhost:27017/")
    return client[db_name]

def get_art_container_size():
    db = get_db_connection()
    query = {
    "messageData.patientIdentifiers": {
        "$elemMatch": {
            "identifierType": 4,
            "voided": 0
        }
    }
    }
    art_containers_count = db.container.count_documents(query)
    return art_containers_count
def get_art_containers():
    db = get_db_connection()
    query = {
    "messageData.patientIdentifiers": {
        "$elemMatch": {
            "identifierType": 4,
            "voided": 0
        }
    }
    }
    art_containers_cusor = db.container.find(query)
    return art_containers_cusor
def get_message_header(doc):
    message_header = doc.get('messageHeader', {})
    return message_header


def get_patient_demographics(doc):
    patient_demographics = doc.get('messageData',{}).get('demographics',{})
    return patient_demographics

def get_patient_identifier(identifier_type_id, doc):
    patient_identifiers = doc.get('messageData',{}).get('patientIdentifiers',{})

    # 2. Check if identifiers is actually a list and not empty
    if not isinstance(patient_identifiers, list) or len(patient_identifiers) == 0:
        return None
    # 3. Search for the specific type that is NOT voided
    for item in patient_identifiers:
        if item.get("identifierType") == identifier_type_id and item.get("voided") == 0:
            return item.get("identifier")

    # 4. Return None if the loop finishes without finding a match
    return None

def get_patient_birthdate(doc):
    patient_demographics = get_patient_demographics(doc)
    return patient_demographics.get('birthdate')

def calculateAge(birthdate: datetime):
    
     if birthdate is None:
        return None

     today = date.today()
     dob = birthdate.date()

     return today.year - dob.year - (
        (today.month, today.day) < (dob.month, dob.day)
     )
        
def get_current_age_at_date_in_months(doc, at_date: Optional[datetime] = None):
    birthdate = get_patient_birthdate(doc)
    if at_date is None:
        at_date = datetime.now()
    
    if birthdate is None:
        return None

    today = at_date.date()
    dob = birthdate.date()

    years_difference = today.year - dob.year
    months_difference = today.month - dob.month
    total_months = years_difference * 12 + months_difference

    # Adjust if the current day is before the birth day in the month
    if today.day < dob.day:
        total_months -= 1
    if total_months > 60:
        return None

    return total_months
def get_viral_load_date_fy_and_quarter(doc, cutoff_datetime: Optional[datetime] = None):
    current_viral_load_obs = get_last_obs_before_date(doc, constants.LAB_FORM_ID, constants.VIRAL_LOAD_CONCEPT_ID, cutoff_datetime)
    if current_viral_load_obs is None:
        return None
    
    viral_load_date = current_viral_load_obs.get("obsDatetime")

    if viral_load_date is None:
        return None
    return get_fy_and_quater_from_date(viral_load_date)

def get_fy_and_quater_from_date(input_date: datetime):
    if input_date is None:
        return None

    # Financial year starts in October
    FY_START_MONTH = 10

    year = input_date.year
    month = input_date.month

    # Determine financial year
    if month >= FY_START_MONTH:
        fy_year = year
        quarter = 1
    elif month >= 1 and month <= 3:
        fy_year = year - 1
        quarter = 2
    elif month >= 4 and month <= 6:
        fy_year = year - 1
        quarter = 3
    else:  # Jul–Sep
        fy_year = year - 1
        quarter = 4

    return f"FY{str(fy_year)[-2:]}Q{quarter}"

def get_current_age_at_date(doc, at_date: Optional[datetime] = None):
    birthdate = get_patient_birthdate(doc)
    if at_date is None:
        at_date = datetime.now()
    
    if birthdate is None:
        return None

    today = at_date.date()
    dob = birthdate.date()

    return today.year - dob.year - (
        (today.month, today.day) < (dob.month, dob.day)
    )
def get_patient_current_age(doc):
    birthdate = get_patient_birthdate(doc)
    return calculateAge(birthdate)

def get_first_weight(doc,cutoff_datetime: Optional[datetime] = None):
    obs = get_first_obs(doc,constants.CARE_CARD_FORM_ID, constants.WEIGHT_KG_CONCEPT_ID)
    weight = obs.get("valueNumeric") if obs else None
    return weight
def get_last_encounter_by_form_id(doc, form_id, cutoff_datetime: Optional[datetime] = None):
    encounter_list = doc.get("messageData", {}).get("encounters", [])
    matching_encounters = []

    # If a cutoff date is not provided use current date as cutoff
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    for encounter in encounter_list:
        if (encounter.get("formId") == form_id and
            encounter.get("voided") ==0):

            encounter_datetime = encounter.get("encounterDatetime")

            if isinstance(encounter_datetime, datetime):
                if encounter_datetime <= cutoff_datetime:
                    matching_encounters.append(encounter)

    if not matching_encounters:
        return None

    # 3. Sort by the actual datetime objects (Newest first)
    matching_encounters.sort(key=lambda x: x.get('encounterDatetime'), reverse=True)
    
    return matching_encounters[0]
def get_last_encounter(doc,cutoff_datetime: Optional[datetime] = None):
    encounter_list = doc.get("messageData", {}).get("encounters", [])
    matching_encounters = []

    # If a cutoff date is not provided use current date as cutoff
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    for encounter in encounter_list:
        if (encounter.get("formId") != constants.CLIENT_TRACKING_DISCONTINUATION_FORM_ID and
            encounter.get("voided") ==0):

            encounter_datetime = encounter.get("encounterDatetime")

            if isinstance(encounter_datetime, datetime):
                if encounter_datetime <= cutoff_datetime:
                    matching_encounters.append(encounter)

    if not matching_encounters:
        return None

    # 3. Sort by the actual datetime objects (Newest first)
    matching_encounters.sort(key=lambda x: x.get('encounterDatetime'), reverse=True)
    
    return matching_encounters[0]
                

def get_last_encounter_date(doc, cutoff_datetime: Optional[datetime] = None):
    encounter = get_last_encounter(doc, cutoff_datetime) 
    encounter_datetime = encounter.get('encounterDatetime') if encounter else None
    return encounter_datetime


def get_last_drug_pickup_duration(doc, cutoff_datetime: Optional[datetime] = None):
    obs = get_last_arv_obs(doc,cutoff_datetime)
    if obs:
        obs_group_id = obs.get("obsId")
        encounter_id = obs.get("encounterId")
        form_id = obs.get("formId")
        obs_medication_duration = get_obs_with_group_id(doc, form_id, encounter_id, constants.ARV_MEDICATION_DURATION_CONCEPT_ID,obs_group_id)
        if obs_medication_duration:
            arv_duration = obs_medication_duration.get("valueNumeric")
            return arv_duration
        else:
            arv_duration = None
            return arv_duration
        
        
    
def get_obs_with_encounter_id(doc, concept_id, encounter_id):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        if (obs.get("encounterId") == encounter_id and
            obs.get("conceptId") == concept_id and
            obs.get("voided") == 0 ):

            matching_obs.append(obs)
            
    if not matching_obs:
        return None
        
    return matching_obs[0]
    
    
def get_obs_with_group_id(doc, form_id, encounter_id, search_obs_concept_id,obs_group_id):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        if (obs.get("formId") == form_id and
            obs.get("encounterId") == encounter_id and
            obs.get("conceptId") == search_obs_concept_id and
            obs.get("obsGroupId") == obs_group_id and
            obs.get("voided") == 0 ):

            # 2. Access the datetime object directly
            obs_dt = obs.get("obsDatetime")
            if isinstance(obs_dt, datetime):
                matching_obs.append(obs)
                
    if not matching_obs:
        return None
    return matching_obs[0]
            
def get_first_obs_with_value(doc,form_id,concept_id, value_coded_arr, earliest_cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0 and
            obs.get("valueCoded") in value_coded_arr):
            
            # 2. Access the datetime object directly
            obs_dt = obs.get("obsDatetime")
            
            # Ensure it is a valid datetime object before comparing
            if isinstance(obs_dt, datetime):
                # 2. Check the optional earliest cutoff
                # If None, the condition is always True. 
                # If provided, obs_dt must be >= cutoff.
                if earliest_cutoff_datetime is None or obs_dt >= earliest_cutoff_datetime:
                    matching_obs.append(obs)
                

    if not matching_obs:
        return None
        
    # 3. Sort by the actual datetime objects (Oldest first)
    matching_obs.sort(key=lambda x: x.get('obsDatetime'))
    
    return matching_obs[0] 
  
def get_first_obs(doc,form_id,concept_id, earliest_cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0):
            
            # 2. Access the datetime object directly
            obs_dt = obs.get("obsDatetime")
            
            # Ensure it is a valid datetime object before comparing
            if isinstance(obs_dt, datetime):
                # 2. Check the optional earliest cutoff
                # If None, the condition is always True. 
                # If provided, obs_dt must be >= cutoff.
                if earliest_cutoff_datetime is None or obs_dt >= earliest_cutoff_datetime:
                    matching_obs.append(obs)
                

    if not matching_obs:
        return None
        
    # 3. Sort by the actual datetime objects (Oldest first)
    matching_obs.sort(key=lambda x: x.get('obsDatetime'))
    
    return matching_obs[0]
                        

    

def get_last_obs_before_date(doc, form_id, concept_id, cutoff_datetime: Optional[datetime] = None):
    """
    Finds the most recent non-voided observation for a specific form and concept
    that occurred on or before the cutoff date.
    
    Args:
        doc (dict): The patient JSON document.
        form_id (int): The specific formId (e.g., 14 for Care Card).
        concept_id (int): The specific conceptId (e.g., 5089 for Weight).
        cutoff_date (datetime): The Python datetime object for the cutoff.
        
    Returns:
        dict: The most recent observation object, or None if not found.
    """
    obs_list = doc.get("messageData", {}).get("obs", [])

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0):
            
            # 2. Access the datetime object directly
            obs_dt = obs.get("obsDatetime")
            
            # Ensure it is a valid datetime object before comparing
            if isinstance(obs_dt, datetime):
                if obs_dt <= cutoff_datetime:
                    matching_obs.append(obs)

    if not matching_obs:
        return None

    # 3. Sort by the actual datetime objects (Newest first)
    matching_obs.sort(key=lambda x: x['obsDatetime'], reverse=True)
    
    return matching_obs[0]

def convert_stringfloat_to_int(value):
    return int(float(value))

def get_nth_obs_of_last_x_obs(doc, form_id, concept_id, n, x, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])

    if not obs_list:
        return None

    matching_obs_list = []

    for e in obs_list:
        # 1. Standard clinical checks
        if (e.get("formId") == form_id and 
            e.get("conceptId") == concept_id and 
            e.get("voided") == 0):
            
            obs_dt = e.get('obsDatetime')
            
            # 2. Check if the date exists and handle the optional cutoff
            if isinstance(obs_dt, datetime):
                # If cutoff is None, we accept all dates. 
                # If cutoff is provided, we check if obs_dt is on or after it.
                if cutoff_datetime is None or obs_dt >= cutoff_datetime:
                    matching_obs_list.append(e)
   
    if not matching_obs_list:
        return None

    # 3. Sort by encounterDatetime (Newest to Oldest)
    # Using .get() for the sort key handles potential missing dates
    matching_obs_list.sort(key=lambda x: x.get('obsDatetime'), reverse=True)

    # 4. Limit to last x observations
    limited_obs_list = matching_obs_list[:x]

    # 5. Return the nth item (Index is n-1)
    # Check if the list is long enough to avoid IndexError
    if len(limited_obs_list) >= n:
        return limited_obs_list[n-1]
    
    return None
def get_nth_obs(doc, form_id, concept_id, n, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])

    if not obs_list:
        return None

    matching_obs_list = []

    for e in obs_list:
        # 1. Standard clinical checks
        if (e.get("formId") == form_id and 
            e.get("conceptId") == concept_id and 
            e.get("voided") == 0):
            
            obs_dt = e.get('obsDatetime')
            
            # 2. Check if the date exists and handle the optional cutoff
            if isinstance(obs_dt, datetime):
                # If cutoff is None, we accept all dates. 
                # If cutoff is provided, we check if obs_dt is on or after it.
                if cutoff_datetime is None or obs_dt >= cutoff_datetime:
                    matching_obs_list.append(e)
   
    if not matching_obs_list:
        return None

    # 3. Sort by encounterDatetime (Oldest to Newest)
    # Using .get() for the sort key handles potential missing dates
    matching_obs_list.sort(key=lambda x: x.get('obsDatetime'))

    # 4. Return the nth item (Index is n-1)
    # Check if the list is long enough to avoid IndexError
    if len(matching_obs_list) >= n:
        return matching_obs_list[n-1]
    
    return None
    
    
def get_nth_encounter(doc, form_id, n):
    """
    Retrieves the nth occurrence of a specific form based on encounterDatetime.
    
    Args:
        doc (dict): The NMRS patient document.
        form_id (int): The ID of the form (e.g., 69 for EAC).
        n (int): The position (1 for first, 2 for second, etc.).
        
    Returns:
        dict: The nth encounter object, or None if it doesn't exist.
    """
    # 1. Access the encounters array
    encounters = doc.get("messageData", {}).get("encounters", [])
    
    # 2. Filter for matching formId and ensure not voided
    matching_encounters = [
        e for e in encounters 
        if e.get("formId") == form_id and e.get("voided") == 0
    ]
    
    if not matching_encounters:
        return None

    # 3. Sort by encounterDatetime (Oldest to Newest)
    # Using .get() for the sort key handles potential missing dates
    matching_encounters.sort(key=lambda x: x.get('encounterDatetime', datetime.min))
    
    # 4. Return the nth item (Index is n-1)
    # Check if the list is long enough to avoid IndexError
    if len(matching_encounters) >= n:
        return matching_encounters[n-1]
    
    return None






# Popular Variables"

def get_nth_pickup_obs_of_last_x_pickups(doc, n, x, cutoff_datetime: Optional[datetime] = None):
    wrapping_arv_obs = get_nth_obs_of_last_x_obs(doc, constants.PHARMACY_FORM_ID, constants.ARV_WRAPPING_CONCEPT_ID, n, x, cutoff_datetime)

    if not wrapping_arv_obs:
        return None
    return wrapping_arv_obs
   
def get_nth_viral_load_obs_of_last_x_viral_loads(doc, n, x, cutoff_datetime: Optional[datetime] = None):
    viral_load_obs = get_nth_obs_of_last_x_obs(doc, constants.LAB_FORM_ID, constants.VIRAL_LOAD_CONCEPT_ID, n, x, cutoff_datetime)

    return viral_load_obs


def get_nth_arv_pickup_obs(doc, n, cutoff_datetime: Optional[datetime] = None): 
    wrapping_arv_obs = get_nth_obs(doc, constants.PHARMACY_FORM_ID, constants.ARV_WRAPPING_CONCEPT_ID, n, cutoff_datetime)

    return wrapping_arv_obs
def get_nth_medication_duration(doc, n, cutoff_datetime: Optional[datetime] = None):    
    wrapping_arv_obs = get_nth_arv_pickup_obs(doc, n, cutoff_datetime)
    if not wrapping_arv_obs:
        return None
    obs_group_id = wrapping_arv_obs.get("obsId")
    encounter_id = wrapping_arv_obs.get("encounterId")
    form_id = wrapping_arv_obs.get("formId")
    medication_duration_obs = get_obs_with_group_id(doc, form_id, encounter_id, constants.ARV_MEDICATION_DURATION_CONCEPT_ID,obs_group_id)
    if not medication_duration_obs:
        return None
    arv_duration = medication_duration_obs.get("valueNumeric")
    return arv_duration
def get_nth_appointment_date(doc, n, cutoff_datetime: Optional[datetime] = None):
    wrapping_arv_obs = get_nth_arv_pickup_obs(doc, n, cutoff_datetime)
    if not wrapping_arv_obs:
        return None
    encounter_id = wrapping_arv_obs.get("encounterId")
    pharmacy_next_appointment_obs = get_obs_with_encounter_id(doc, constants.NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id)
    if not pharmacy_next_appointment_obs:
        return None
    next_appointment_date = pharmacy_next_appointment_obs.get("valueDatetime")
    return next_appointment_date

def get_nth_arv_pickup_date(doc, n, cutoff_datetime: Optional[datetime] = None):
    wrapping_arv_obs = get_nth_arv_pickup_obs(doc, n, cutoff_datetime)
    if not wrapping_arv_obs:
        return None
    arv_pickup_date = wrapping_arv_obs.get("obsDatetime")
    return arv_pickup_date

def get_clinical_next_appointment_date(doc, cutoff_datetime: Optional[datetime] = None):
    clinical_next_appointment_obs = get_last_obs_before_date(doc, constants.CARE_CARD_FORM_ID, constants.NEXT_APPOINTMENT_DATE_CONCEPT_ID, cutoff_datetime)
    
    if not clinical_next_appointment_obs:
        return None
    next_appointment_date = clinical_next_appointment_obs.get("valueDatetime")
    return next_appointment_date


def get_pharmacy_next_appointment_date(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_pickup_obs:
        return None
    encounter_id= last_arv_pickup_obs.get("encounterId")
    pharmacy_next_appointment_obs = get_obs_with_encounter_id(doc, constants.NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id)
    if not pharmacy_next_appointment_obs:
        return None
    next_appointment_date = pharmacy_next_appointment_obs.get("valueDatetime")
    return next_appointment_date

def get_facility_dsd_model(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_obs:
        return None
    encounter_id= last_arv_obs.get("encounterId")
    dsd_model_obs = get_obs_with_encounter_id(doc, constants.FACILITY_DSD_MODEL_CONCEPT_ID, encounter_id)
    if not dsd_model_obs:
        return None 
    dsd_model = dsd_model_obs.get('variableValue')
    return dsd_model

def get_ddd_dsd_model(doc, cutoff_datetime: Optional[datetime] = None): 
    last_arv_obs = last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_obs:
        return None
    encounter_id= last_arv_obs.get("encounterId")
    dsd_model_obs = get_obs_with_encounter_id(doc, constants.DDD_DSD_MODEL_CONCEPT_ID, encounter_id)
    if not dsd_model_obs:
        return None 
    dsd_model = dsd_model_obs.get('variableValue')
    return dsd_model

def get_mmd_type(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_obs = last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_obs:
        return None
    encounter_id= last_arv_obs.get("encounterId")
    dsd_model_obs = get_obs_with_encounter_id(doc, constants.MMD_CONCEPT_ID, encounter_id)
    if not dsd_model_obs:
        return None 
    dsd_model = dsd_model_obs.get('variableValue')
    return dsd_model

def get_last_dsd_model(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_obs = last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_obs:
        return None
    encounter_id= last_arv_obs.get("encounterId")
    dsd_model_obs = get_obs_with_encounter_id(doc, constants.DSD_MODEL_CONCEPT_ID, encounter_id)
    if not dsd_model_obs:
        return None 
    dsd_model = dsd_model_obs.get('variableValue')
    return dsd_model

def get_current_art_status(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_pickup_obs:
        return None
    
    obs_group_id= last_arv_pickup_obs.get("obsId")
    encounter_id = last_arv_pickup_obs.get("encounterId")
    form_id = last_arv_pickup_obs.get("formId")

    medication_duration_obs = get_obs_with_group_id(doc, form_id, encounter_id, constants.ARV_MEDICATION_DURATION_CONCEPT_ID, obs_group_id)
    
    if not medication_duration_obs:
        return None
    
    last_arv_pickup_date = last_arv_pickup_obs.get("obsDatetime")
    arv_duration_days = int(float(medication_duration_obs.get("valueNumeric")))

    ltfu_date = last_arv_pickup_date + pd.Timedelta(days=arv_duration_days + constants.DAYS_BEFORE_LTFU)

    patient_outcome = get_patient_outcome(doc, cutoff_datetime)

    if patient_outcome is not None:
        return patient_outcome
    
    if ltfu_date < (cutoff_datetime or datetime.now()):
        return "InActive"
    else:
        return "Active"
    

def get_outcome_date(doc, cutoff_datetime: Optional[datetime] = None):
    reson_for_termination_obs = get_last_obs_before_date(doc, constants.CLIENT_TRACKING_DISCONTINUATION_FORM_ID, constants.REASON_FOR_TERMINATION_CONCEPT_ID, cutoff_datetime)
    if not reson_for_termination_obs:
        return None
    termination_date = reson_for_termination_obs.get("obsDatetime") 
    return termination_date

def get_last_sample_taken_date(doc, cutoff_datetime: Optional[datetime] = None):
    viral_load_sample_obs = get_last_obs_before_date(doc, constants.LAB_FORM_ID, constants.SAMPLE_COLLECTION_DATE_CONCEPT_ID, cutoff_datetime)
    if not viral_load_sample_obs:
        return None
    sample_collection_date = viral_load_sample_obs.get('valueDatetime')
    return sample_collection_date


def get_current_viral_load_indication(doc, cutoff_datetime: Optional[datetime] = None):
    viral_load_indication_obs = get_last_obs_before_date(doc, constants.LAB_FORM_ID, constants.VIRAL_LOAD_INDICATION_CONCEPT_ID, cutoff_datetime)
    if not viral_load_indication_obs:
        return None
    viral_load_indication = viral_load_indication_obs.get('variableValue')
    return viral_load_indication

def get_last_eac_session_type(doc, cutoff_datetime: Optional[datetime] = None):
    last_eac_encounter = get_last_encounter_by_form_id(doc, constants.EAC_FORM_ID, cutoff_datetime)
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_session_type_obs = get_obs_with_encounter_id(doc, constants.EAC_SESSION_TYPE_CONCEPT_ID, last_eac_encounter_id)
    eac_session_type = last_eac_session_type_obs.get("variableValue") if last_eac_session_type_obs else None
    return eac_session_type

def get_current_viral_load_value(doc, cutoff_datetime: Optional[datetime] = None):
    viral_load_obs = get_last_viral_load_obs_before(doc, cutoff_datetime)
    viral_load = viral_load_obs.get('valueNumeric') if viral_load_obs else None
    return viral_load

def get_current_viral_load_encounter_date(doc, cutoff_datetime: Optional[datetime] = None):
    viral_load_obs = get_last_viral_load_obs_before(doc, cutoff_datetime)
    if not viral_load_obs:
        return None
    viral_load_date = viral_load_obs.get('obsDatetime')
    return viral_load_date

def get_current_viral_load_sample_date(doc, cutoff_datetime: Optional[datetime] = None):
    viral_load_obs = get_last_viral_load_obs_before(doc, cutoff_datetime)
    if not viral_load_obs:
        return None
    encounter_id = viral_load_obs.get('encounterId')
    sample_collection_obs = get_obs_with_encounter_id(doc, constants.SAMPLE_COLLECTION_DATE_CONCEPT_ID, encounter_id)
    sample_collection_date = sample_collection_obs.get('valueDatetime') if sample_collection_obs else None
    return sample_collection_date

def get_last_eac_comments(doc, cutoff_datetime: Optional[datetime] = None):
    last_eac_encounter = get_last_encounter_by_form_id(doc, constants.EAC_FORM_ID, cutoff_datetime)
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_comments_obs = get_obs_with_encounter_id(doc, constants.EAC_ADHERENCE_COMMENTS_CONCEPT_ID, last_eac_encounter_id)
    eac_comments = last_eac_comments_obs.get("valueText") if last_eac_comments_obs else None
    return eac_comments

def get_last_eac_session_type_datetime(doc, cutoff_datetime: Optional[datetime] = None):
    last_eac_encounter = get_last_encounter_by_form_id(doc, constants.EAC_FORM_ID, cutoff_datetime)
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_session_type_obs = get_obs_with_encounter_id(doc, constants.EAC_SESSION_TYPE_CONCEPT_ID, last_eac_encounter_id)
    eac_session_type_datetime = last_eac_session_type_obs.get("obsDatetime") if last_eac_session_type_obs else None
    return eac_session_type_datetime

def get_last_eac_barriers_to_adherence(doc, cutoff_datetime: Optional[datetime] = None):
    last_eac_encounter = get_last_encounter_by_form_id(doc, constants.EAC_FORM_ID, cutoff_datetime)
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_barriers_to_adherence_obs = get_obs_with_encounter_id(doc, constants.EAC_BARRIERS_TO_ADHERENCE_CONCEPT_ID, last_eac_encounter_id)
    eac_barriers_to_adherence = last_eac_barriers_to_adherence_obs.get("variableValue") if last_eac_barriers_to_adherence_obs else None
    return eac_barriers_to_adherence

def get_last_eac_regimen_plan(doc, cutoff_datetime: Optional[datetime] = None):
    last_eac_encounter = get_last_encounter_by_form_id(doc, constants.EAC_FORM_ID, cutoff_datetime)
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_regimen_plan_obs = get_obs_with_encounter_id(doc, constants.EAC_REGIMEN_PLAN_CONCEPT_ID, last_eac_encounter_id)
    eac_regimen_plan = last_eac_regimen_plan_obs.get("variableValue") if last_eac_regimen_plan_obs else None
    return eac_regimen_plan
def get_last_eac_followup_date(doc, cutoff_datetime: Optional[datetime] = None):
    last_eac_encounter = get_last_encounter_by_form_id(doc, constants.EAC_FORM_ID, cutoff_datetime)
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_followup_date_obs = get_obs_with_encounter_id(doc, constants.EAC_FOLLOWUP_DATE_CONCEPT_ID, last_eac_encounter_id)
    eac_followup_date = last_eac_followup_date_obs.get("valueDatetime") if last_eac_followup_date_obs else None
    return eac_followup_date

def get_edd_for_last_pregnancy(doc, cutoff_datetime: Optional[datetime] = None):
    pregnancy_status_obs = get_last_obs_before_date(doc, constants.CARE_CARD_FORM_ID, constants.PREGNANCY_STATUS_CONCEPT_ID, cutoff_datetime)
    if not pregnancy_status_obs:
        return None
    encounter_id = pregnancy_status_obs.get("encounterId")
    edd_obs = get_obs_with_encounter_id(doc, constants.EDD_CONCEPT_ID, encounter_id)
    edd_date = edd_obs.get("valueDatetime") if edd_obs else None
    return edd_date

def get_current_pregnancy_status(doc, cutoff_datetime: Optional[datetime] = None):
    obs = get_last_obs_before_date(doc, constants.CARE_CARD_FORM_ID, constants.PREGNANCY_STATUS_CONCEPT_ID, cutoff_datetime)
    pregnancy_status = obs.get("variableValue") if obs else None
    return pregnancy_status
def get_current_pregnancy_status_datetime(doc, cutoff_datetime: Optional[datetime] = None):
    obs = get_last_obs_before_date(doc, constants.CARE_CARD_FORM_ID, constants.PREGNANCY_STATUS_CONCEPT_ID, cutoff_datetime)
    pregnancy_status_datetime = obs.get("obsDatetime") if obs else None
    return pregnancy_status_datetime

def get_min_second_line_regimen_date(doc, cutoff_datetime: Optional[datetime] = None):
    second_line_concept_arr = [constants.CHILD_2ND_LINE_REGIMEN_CONCEPT_ID, constants.ADULT_2ND_LINE_REGIMEN_CONCEPT_ID]
    first_second_line_obs = get_first_obs_with_value(doc, constants.PHARMACY_FORM_ID,  second_line_concept_arr, cutoff_datetime)
    regimen_date = first_second_line_obs.get("obsDatetime") if first_second_line_obs else None
    return regimen_date
def get_min_third_line_regimen_date(doc, cutoff_datetime: Optional[datetime] = None):
    third_line_concept_arr = [constants.CHILD_3RD_LINE_REGIMEN_CONCEPT_ID, constants.ADULT_3RD_LINE_REGIMEN_CONCEPT_ID]
    first_third_line_obs = get_first_obs_with_value(doc, constants.PHARMACY_FORM_ID,  third_line_concept_arr, cutoff_datetime)
    regimen_date = first_third_line_obs.get("obsDatetime") if first_third_line_obs else None
    return regimen_date

def get_current_regimen(doc, cutoff_datetime: Optional[datetime] = None):
    current_regimen_line_obs = get_last_obs_before_date(doc, constants.PHARMACY_FORM_ID, constants.CURRENT_REGIMEN_LINE_CONCEPT_ID, cutoff_datetime)
    if(current_regimen_line_obs is None):
        return None
    valueCoded = current_regimen_line_obs.get("valueCoded") 
    encounter_id = current_regimen_line_obs.get("encounterId")
    current_regimen_obs = get_obs_with_encounter_id(doc, valueCoded, encounter_id)
    current_regimen = current_regimen_obs.get("variableValue") if current_regimen_obs else None
    return current_regimen

def get_current_regimen_line(doc, cutoff_datetime: Optional[datetime] = None):
    obs = get_last_obs_before_date(doc, constants.PHARMACY_FORM_ID, constants.CURRENT_REGIMEN_LINE_CONCEPT_ID, cutoff_datetime)
    current_regimen_line = obs.get("variableValue") if obs else None
    return current_regimen_line

def get_patient_outcome(doc, cutoff_datetime: Optional[datetime] = None ):
    patient_outcome_obs = get_last_obs_before_date(doc, constants.CLIENT_TRACKING_DISCONTINUATION_FORM_ID, 
                                                      constants.REASON_FOR_TERMINATION_CONCEPT_ID, cutoff_datetime)
    if not patient_outcome_obs:
        return None

    patient_outcome = patient_outcome_obs.get('variableValue')
    return patient_outcome
    

def get_nth_viral_reported_date(doc, n , cutoff_datetime: Optional[datetime] = None):
    nth_viral_load_obs = get_nth_obs(doc, constants.LAB_FORM_ID, constants.VIRAL_LOAD_CONCEPT_ID, n, cutoff_datetime)
    
    if not nth_viral_load_obs:
        return None
    
    encounter_id = nth_viral_load_obs.get('encounterId') 
    
    reported_date_obs = get_obs_with_encounter_id(doc, constants.VIRAL_LOAD_REPORTED_DATE_CONCEPT_ID, encounter_id)
    reported_date = reported_date_obs.get('valueDatetime') if reported_date_obs else None
    return reported_date


    
def get_reported_date_of_viral_load_before_first_eac(doc):
    viral_load_obs = get_last_viral_load_obs_before_first_eac(doc)
    if not viral_load_obs:
        return None
    encounter_id = viral_load_obs.get('encounterId')
    reported_date_obs = get_obs_with_encounter_id(doc, constants.VIRAL_LOAD_REPORTED_DATE_CONCEPT_ID, encounter_id)
    reported_date = reported_date_obs.get('valueDatetime') if reported_date_obs else None
    return reported_date
    
def get_sample_collection_date_of_viral_load_before_first_eac(doc):
    viral_load_obs = get_last_viral_load_obs_before_first_eac(doc)
    if not viral_load_obs:
        return None
    encounter_id = viral_load_obs.get('encounterId')
    sample_collection_date_obs = get_obs_with_encounter_id(doc, constants.SAMPLE_COLLECTION_DATE_CONCEPT_ID, encounter_id)
    sample_collection_date = sample_collection_date_obs.get('valueDatetime') if sample_collection_date_obs else None

    return sample_collection_date
    
def get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_obs):
    if not viral_load_obs:
        return None
    encounter_id = viral_load_obs.get('encounterId')
    sample_collection_date_obs = get_obs_with_encounter_id(doc, constants.SAMPLE_COLLECTION_DATE_CONCEPT_ID, encounter_id)
    #sample_collection_date = sample_collection_date_obs.get('valueDatetime') if sample_collection_date_obs else None

    return sample_collection_date_obs
    
def get_last_viral_load_before_first_eac_value(doc):
    viral_load_obs = get_last_viral_load_obs_before_first_eac(doc)
    
    viral_load = viral_load_obs.get('valueNumeric') if viral_load_obs else None
    return viral_load

def get_last_viral_load_before_first_eac_date(doc):
    viral_load_obs = get_last_viral_load_obs_before_first_eac(doc)
    viral_load_date = viral_load_obs.get('obsDatetime') if viral_load_obs else None
    return viral_load_date

def get_last_viral_load_obs_before_first_eac(doc):
    
    eac1_date = get_eac_date(1, doc)

    if not eac1_date:
        return None
    
    viral_load_obs = get_last_obs_before_date(doc, constants.LAB_FORM_ID, constants.VIRAL_LOAD_CONCEPT_ID , eac1_date)
    
    return viral_load_obs
    
    
def get_last_viral_load_obs_before(doc, cutoff_datetime):
    viral_load_obs = get_last_obs_before_date(doc, constants.LAB_FORM_ID, constants.VIRAL_LOAD_CONCEPT_ID , cutoff_datetime)
    
    return viral_load_obs

    
def get_eac_date(n, doc):
    eacn = get_nth_encounter(doc, constants.EAC_FORM_ID, n)
    eacn_date = eacn.get("encounterDatetime") if eacn else None
    return eacn_date

def get_pill_balance(doc,cutoff_datetime: Optional[datetime] = None):
    
    # 1. Get the anchor ARV observation
    latest_arv_obs = get_last_arv_obs(doc, cutoff_datetime)

    # 2. Guard Clause: If no ARV visit found, exit early
    if not latest_arv_obs:
        return None

    # 3. Retrieve the specific pill balance from the same encounter
    encounter_id = latest_arv_obs.get('encounterId')
    obs_pill_balance = get_obs_with_encounter_id(doc, constants.PILL_BALANCE_CONCEPT_ID, encounter_id)
    pill_balance = obs_pill_balance.get('variableValue') if obs_pill_balance else None

    return pill_balance
    
   

    

def get_last_arv_obs(doc,cutoff_datetime: Optional[datetime] = None):
   obs = get_last_obs_before_date(doc, constants.PHARMACY_FORM_ID, constants.ARV_WRAPPING_CONCEPT_ID,cutoff_datetime) 
   return obs

def get_last_arv_pickup_date(doc, cutoff_datetime: Optional[datetime] = None):
    obs = get_last_arv_obs(doc,cutoff_datetime)
    if not obs:
        return None
    last_pickup_date = obs.get("obsDatetime")
    return last_pickup_date
    
    
    

def get_art_start_date(doc,cutoff_datetime: Optional[datetime] = None):
    obs = get_last_obs_before_date(doc, constants.ART_COMMENCEMENT_FORM_ID, constants.ART_START_DATE_CONCEPT_ID,cutoff_datetime)
    # Result if true | condition | result if false
    art_start_date = obs.get("valueDatetime") if obs else None
    return art_start_date

def get_date_transferred_in(doc,cutoff_datetime: Optional[datetime] = None):
    obs = get_last_obs_before_date(doc, constants.HIV_ENROLLMENT_FORM_ID, constants.DATE_TRANSFERED_IN_CONCEPT_ID,cutoff_datetime)
    # Result if true | condition | result if false
    date_transferred_in = obs.get("valueDatetime") if obs else None
    return date_transferred_in
    

def get_care_entry_point(doc,cutoff_datetime: Optional[datetime] = None):
    obs = get_last_obs_before_date(doc, constants.HIV_ENROLLMENT_FORM_ID, constants.CARE_ENTRY_POINT_CONCEPT_ID,cutoff_datetime)
    care_entry_point = obs.get("variableValue") if obs else None
    return care_entry_point

def get_prior_art(doc,cutoff_datetime: Optional[datetime] = None):
    obs = get_last_obs_before_date(doc, constants.HIV_ENROLLMENT_FORM_ID, constants.PRIOR_ART_CONCEPT_ID,cutoff_datetime)
    prior_art = obs.get("variableValue") if obs else None
    return prior_art
    
def get_kp_type(doc,cutoff_datetime: Optional[datetime] = None):
    obs = get_last_obs_before_date(doc, constants.HIV_ENROLLMENT_FORM_ID, constants.KP_TYPE_CONCEPT_ID,cutoff_datetime)
    kp_type = obs.get("variableValue") if obs else None
    return kp_type
       
def get_month_diff(datetime1: datetime, datetime2: datetime) -> int:
    """
    Returns the number of whole calendar months between datetime1 and datetime2.
    Result is positive if datetime2 >= datetime1, negative otherwise.
    """
    if datetime1 > datetime2:
        datetime1, datetime2 = datetime2, datetime1
        sign = -1
    else:
        sign = 1

    months = (datetime2.year - datetime1.year) * 12 + (datetime2.month - datetime1.month)

    # Adjust if end day is before start day
    if datetime2.day < datetime1.day:
        months -= 1

    return months * sign

def get_year_diff(datetime1: datetime, datetime2: datetime) -> int:
    """
    Returns the number of whole calendar years between datetime1 and datetime2.
    Result is positive if datetime2 >= datetime1, negative otherwise.
    """
    if datetime1 > datetime2:
        datetime1, datetime2 = datetime2, datetime1
        sign = -1
    else:
        sign = 1

    years = datetime2.year - datetime1.year

    # Adjust if end date is before anniversary
    if (datetime2.month, datetime2.day) < (datetime1.month, datetime1.day):
        years -= 1

    return years * sign


    
def get_age_art_start_months(doc,cutoff_datetime: Optional[datetime] = None):
    art_start_date = get_art_start_date(doc,cutoff_datetime)
    if art_start_date is None:
        return None
    birthdate = get_patient_birthdate(doc)
    if all([art_start_date, birthdate]):
        age_months = get_month_diff(birthdate, art_start_date)
    else:
        return None
        
    return age_months
            
def get_age_art_start_years(doc,cutoff_datetime: Optional[datetime] = None):
    art_start_date = get_art_start_date(doc,cutoff_datetime)
    if art_start_date is None:
        return None
    birthdate = get_patient_birthdate(doc)
    if all([art_start_date, birthdate]):
        age_years = get_year_diff(birthdate, art_start_date)
    else:
        return None
    return age_years
    
def get_pediatric_age_art_start_months(doc):
    age_months = get_age_art_start_months(doc)
    if age_months is None:
        return None
    
    if age_months < 60:
        return age_months
    else:
        return None
def get_months_on_art(doc, cutoff_datetime: Optional[datetime] = None):
    art_start_date = get_art_start_date(doc,cutoff_datetime)
    if art_start_date is None:
        return None
    
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    months_on_art=get_month_diff(art_start_date, cutoff_datetime)
    return months_on_art
    
    
    