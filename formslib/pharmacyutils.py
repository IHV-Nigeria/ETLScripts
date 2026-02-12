from typing import Final, Optional
from datetime import datetime, date
from utils import commonutils
import utils.obsutils as obsutils
import formslib.ctdutils as ctdutils
import pandas as pd

# Constants on Pharmacy
PHARMACY_FORM_ID = 27
ARV_MEDICATION_DURATION_CONCEPT_ID = 159368
ARV_WRAPPING_CONCEPT_ID = 162240

PREGNANCY_STATUS_CONCEPT_ID = 165050
DSD_STATUS_CONCEPT_ID = 167126
DSD_MODEL_CONCEPT_ID = 166148
FACILITY_DSD_MODEL_CONCEPT_ID = 166276  
DDD_DSD_MODEL_CONCEPT_ID = 166363
MMD_CONCEPT_ID = 166278

OI_DRUG_CONCEPT_ID = 165727
ISONIAZID_PROPHYLAXIS_CONCEPT_ID = 1679

CURRENT_REGIMEN_LINE_CONCEPT_ID =  	165708
CHILD_2ND_LINE_REGIMEN_CONCEPT_ID = 164514
ADULT_2ND_LINE_REGIMEN_CONCEPT_ID = 164513
ADULT_3RD_LINE_REGIMEN_CONCEPT_ID = 165702
CHILD_3RD_LINE_REGIMEN_CONCEPT_ID = 165703
ADULT_1ST_LINE_REGIMEN_CONCEPT_ID = 164506
CHILD_FIRST_LINE_REGIMEN_CONCEPT_ID = 164507

PILL_BALANCE_CONCEPT_ID = 166406
DAYS_BEFORE_LTFU: Final[int] = 28

NEXT_APPOINTMENT_DATE_CONCEPT_ID = 5096




def get_last_drug_pickup_duration(doc,last_arv_obs):
    if last_arv_obs is None:
        return None 
    
    obs_group_id = last_arv_obs.get("obsId")
    encounter_id = last_arv_obs.get("encounterId")
    form_id = last_arv_obs.get("formId")

    obs_medication_duration = obsutils.get_obs_with_group_id(doc, form_id, encounter_id, ARV_MEDICATION_DURATION_CONCEPT_ID,obs_group_id)
    if obs_medication_duration is None:
        return None
    arv_duration = obs_medication_duration.get("valueNumeric")
    if arv_duration is None:
        return None
           
    return arv_duration
        

def get_nth_pickup_isoniazid_prophylaxis_obs_of_last_x_pickups(doc, n, x, cutoff_datetime: Optional[datetime] = None):
    inh_pickup_obs = obsutils.get_nth_obs_of_last_x_obs_with_valuecoded(doc, PHARMACY_FORM_ID, OI_DRUG_CONCEPT_ID,[ISONIAZID_PROPHYLAXIS_CONCEPT_ID], n, x, cutoff_datetime)

    if not inh_pickup_obs:
        return None
      
    return inh_pickup_obs
def get_last_isoniazid_prophylaxis_pickup_obs(doc, cutoff_datetime: Optional[datetime] = None):
    inh_pickup_obs = obsutils.get_last_obs_with_valuecoded_before_date(doc, PHARMACY_FORM_ID, OI_DRUG_CONCEPT_ID,[ISONIAZID_PROPHYLAXIS_CONCEPT_ID], cutoff_datetime)

    if not inh_pickup_obs:
        return None
      
    return inh_pickup_obs

def get_nth_pickup_obs_of_last_x_pickups(doc, n, x, cutoff_datetime: Optional[datetime] = None):
    wrapping_arv_obs = obsutils.get_nth_obs_of_last_x_obs(doc, PHARMACY_FORM_ID, ARV_WRAPPING_CONCEPT_ID, n, x, cutoff_datetime)

    if not wrapping_arv_obs:
        return None
    return wrapping_arv_obs

def get_nth_arv_pickup_obs(doc, n, cutoff_datetime: Optional[datetime] = None): 
    wrapping_arv_obs = obsutils.get_nth_obs(doc, PHARMACY_FORM_ID, ARV_WRAPPING_CONCEPT_ID, n, cutoff_datetime)

    return wrapping_arv_obs

def get_nth_medication_duration(doc, nth_wrapping_arv_obs):    
    if nth_wrapping_arv_obs is None:
        return None
    
    obs_group_id = nth_wrapping_arv_obs.get("obsId")
    encounter_id = nth_wrapping_arv_obs.get("encounterId")
    form_id = nth_wrapping_arv_obs.get("formId")
    medication_duration_obs = obsutils.get_obs_with_group_id(doc, form_id, encounter_id, ARV_MEDICATION_DURATION_CONCEPT_ID,obs_group_id)
    if not medication_duration_obs:
        return None
    arv_duration = medication_duration_obs.get("valueNumeric")
    return arv_duration

def get_nth_appointment_date(doc, nth_wrapping_arv_obs):
    if nth_wrapping_arv_obs is None:
        return None
    
    encounter_id = nth_wrapping_arv_obs.get("encounterId")
    pharmacy_next_appointment_obs = obsutils.get_obs_with_encounter_id(doc, NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id)
    if not pharmacy_next_appointment_obs:
        return None
    next_appointment_date = pharmacy_next_appointment_obs.get("valueDatetime")
    return next_appointment_date

def get_facility_dsd_model(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_obs:
        return None
    encounter_id= last_arv_obs.get("encounterId")
    dsd_model_obs = obsutils.get_obs_with_encounter_id(doc, FACILITY_DSD_MODEL_CONCEPT_ID, encounter_id)
    if not dsd_model_obs:
        return None 
    dsd_model = dsd_model_obs.get('variableValue')
    return dsd_model

def get_ddd_dsd_model(doc, cutoff_datetime: Optional[datetime] = None): 
    last_arv_obs = last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_obs:
        return None
    encounter_id= last_arv_obs.get("encounterId")
    dsd_model_obs = obsutils.get_obs_with_encounter_id(doc, DDD_DSD_MODEL_CONCEPT_ID, encounter_id)
    if not dsd_model_obs:
        return None 
    dsd_model = dsd_model_obs.get('variableValue')
    return dsd_model

def get_mmd_type(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_obs = last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_obs:
        return None
    encounter_id= last_arv_obs.get("encounterId")
    dsd_model_obs = obsutils.get_obs_with_encounter_id(doc, MMD_CONCEPT_ID, encounter_id)
    if not dsd_model_obs:
        return None 
    dsd_model = dsd_model_obs.get('variableValue')
    return dsd_model

def get_last_dsd_model(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_obs = last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_obs:
        return None
    encounter_id= last_arv_obs.get("encounterId")
    dsd_model_obs = obsutils.get_obs_with_encounter_id(doc, DSD_MODEL_CONCEPT_ID, encounter_id)
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

    medication_duration_obs = obsutils.get_obs_with_group_id(doc, form_id, encounter_id, ARV_MEDICATION_DURATION_CONCEPT_ID, obs_group_id)
    
    if not medication_duration_obs:
        return None
    
    last_arv_pickup_date = last_arv_pickup_obs.get("obsDatetime")
    arv_duration_days = int(float(medication_duration_obs.get("valueNumeric")))

    ltfu_date = last_arv_pickup_date + pd.Timedelta(days=arv_duration_days + DAYS_BEFORE_LTFU)

    patient_outcome = ctdutils.get_patient_outcome(doc, cutoff_datetime)

    if patient_outcome is not None:
        return patient_outcome
    
    if ltfu_date < (cutoff_datetime or datetime.now()):
        return "InActive"
    else:
        return "Active"
    

def get_pill_balance(doc, last_arv_obs, cutoff_datetime: Optional[datetime] = None):
    
    

    # 2. Guard Clause: If no ARV visit found, exit early
    if last_arv_obs is None:
        return None

    # 3. Retrieve the specific pill balance from the same encounter
    encounter_id = last_arv_obs.get('encounterId')
    obs_pill_balance = obsutils.get_obs_with_encounter_id(doc, PILL_BALANCE_CONCEPT_ID, encounter_id)
    pill_balance = obs_pill_balance.get('variableValue') if obs_pill_balance else None

    return pill_balance
    
       
def get_last_arv_obs(doc,cutoff_datetime: Optional[datetime] = None):
   obs = obsutils.get_last_obs_before_date(doc, PHARMACY_FORM_ID, ARV_WRAPPING_CONCEPT_ID,cutoff_datetime) 
   return obs

def get_last_arv_pickup_date(doc, cutoff_datetime: Optional[datetime] = None):
    obs = get_last_arv_obs(doc,cutoff_datetime)
    if not obs:
        return None
    last_pickup_date = obs.get("obsDatetime")
    return commonutils.validate_date(last_pickup_date)

def get_min_second_line_regimen_date(doc, cutoff_datetime: Optional[datetime] = None):
    second_line_concept_arr = [CHILD_2ND_LINE_REGIMEN_CONCEPT_ID, ADULT_2ND_LINE_REGIMEN_CONCEPT_ID]
    first_second_line_obs = obsutils.get_first_obs_with_value(doc, PHARMACY_FORM_ID, CURRENT_REGIMEN_LINE_CONCEPT_ID, second_line_concept_arr, cutoff_datetime)
    regimen_date = first_second_line_obs.get("obsDatetime") if first_second_line_obs else None
    return commonutils.validate_date(regimen_date)  
def get_min_third_line_regimen_date(doc, cutoff_datetime: Optional[datetime] = None):
    third_line_concept_arr = [CHILD_3RD_LINE_REGIMEN_CONCEPT_ID, ADULT_3RD_LINE_REGIMEN_CONCEPT_ID]
    first_third_line_obs = obsutils.get_first_obs_with_value(doc, PHARMACY_FORM_ID, CURRENT_REGIMEN_LINE_CONCEPT_ID,  third_line_concept_arr, cutoff_datetime)
    regimen_date = first_third_line_obs.get("obsDatetime") if first_third_line_obs else None
    return commonutils.validate_date(regimen_date)  

def get_current_regimen(doc, cutoff_datetime: Optional[datetime] = None):
    current_regimen_line_obs = obsutils.get_last_obs_before_date(doc, PHARMACY_FORM_ID, CURRENT_REGIMEN_LINE_CONCEPT_ID, cutoff_datetime)
    if(current_regimen_line_obs is None):
        return None
    valueCoded = current_regimen_line_obs.get("valueCoded") 
    encounter_id = current_regimen_line_obs.get("encounterId")
    current_regimen_obs = obsutils.get_obs_with_encounter_id(doc, valueCoded, encounter_id)
    current_regimen = current_regimen_obs.get("variableValue") if current_regimen_obs else None
    return current_regimen

def get_current_regimen_line(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, PHARMACY_FORM_ID, CURRENT_REGIMEN_LINE_CONCEPT_ID, cutoff_datetime)
    current_regimen_line = obs.get("variableValue") if obs else None
    return current_regimen_line

def get_pharmacy_next_appointment_date(doc, cutoff_datetime: Optional[datetime] = None):
    last_arv_pickup_obs = get_last_arv_obs(doc, cutoff_datetime)
    if not last_arv_pickup_obs:
        return None
    encounter_id= last_arv_pickup_obs.get("encounterId")
    pharmacy_next_appointment_obs = obsutils.get_obs_with_encounter_id(doc, NEXT_APPOINTMENT_DATE_CONCEPT_ID, encounter_id)
    if not pharmacy_next_appointment_obs:
        return None
    next_appointment_date = pharmacy_next_appointment_obs.get("valueDatetime")
    return commonutils.validate_date(next_appointment_date) 
