from typing import Optional
from datetime import datetime, date
from utils import commonutils
import utils.obsutils as obsutils

from utils.obsutils import get_first_obs

# Constants on Care Card
CARE_CARD_FORM_ID = 14

# Care Card Concept IDs

PREGNANCY_STATUS_CONCEPT_ID = 165050
LMP_DATE_CONCEPT_ID = 1427
GESTATION_WEEKS_CONCEPT_ID = 1438
EDD_CONCEPT_ID = 5596
WEIGHT_KG_CONCEPT_ID = 5089
HEIGHT_CM_CONCEPT_ID = 5090
TEMPERATURE_CONCEPT_ID = 5088
SYSTOLIC_BP_CONCEPT_ID = 5085
DIASTOLIC_BP_CONCEPT_ID = 5086
BMI_CONCEPT_ID = 1343
MUAC_CHILD_CONCEPT_ID = 165935
FAMILY_PLANNING_METHOD_CONCEPT_ID = 374
FUNCTIONAL_STATUS_CONCEPT_ID = 165039
WHO_STAGE_CONCEPT_ID = 5356
TB_STATUS_CONCEPT_ID = 1659

CURRENT_REGIMEN_LINE_CONCEPT_ID =  	165708
CHILD_2ND_LINE_REGIMEN_CONCEPT_ID = 164514
ADULT_2ND_LINE_REGIMEN_CONCEPT_ID = 164513
ADULT_3RD_LINE_REGIMEN_CONCEPT_ID = 165702
CHILD_3RD_LINE_REGIMEN_CONCEPT_ID = 165703


NEXT_APPOINTMENT_DATE_CONCEPT_ID = 5096



def get_first_weight(doc,cutoff_datetime: Optional[datetime] = None):
    obs = get_first_obs(doc,CARE_CARD_FORM_ID, WEIGHT_KG_CONCEPT_ID)
    weight = obs.get("valueNumeric") if obs else None
    return weight

def get_current_pregnancy_status_obs(doc, cutoff_datetime: Optional[datetime] = None): 
    pregnancy_status_obs=obsutils.get_last_obs_before_date(doc,CARE_CARD_FORM_ID,PREGNANCY_STATUS_CONCEPT_ID,cutoff_datetime)
    return pregnancy_status_obs

     

def get_current_pregnancy_status(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, CARE_CARD_FORM_ID, PREGNANCY_STATUS_CONCEPT_ID, cutoff_datetime)
    pregnancy_status = obs.get("variableValue") if obs else None
    return pregnancy_status
def get_current_pregnancy_status_datetime(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, CARE_CARD_FORM_ID, PREGNANCY_STATUS_CONCEPT_ID, cutoff_datetime)
    pregnancy_status_datetime = obs.get("obsDatetime") if obs else None
    return pregnancy_status_datetime

def get_edd_for_last_pregnancy(doc, pregnancy_status_obs):
    if not pregnancy_status_obs:
        return None
    encounter_id = pregnancy_status_obs.get("encounterId")
    edd_obs = obsutils.get_obs_with_encounter_id(doc, EDD_CONCEPT_ID, encounter_id)
    return edd_obs

def get_clinical_next_appointment_date(doc, cutoff_datetime: Optional[datetime] = None):
    clinical_next_appointment_obs = obsutils.get_last_obs_before_date(doc, CARE_CARD_FORM_ID, NEXT_APPOINTMENT_DATE_CONCEPT_ID, cutoff_datetime)
    
    if not clinical_next_appointment_obs:
        return None
    next_appointment_date = clinical_next_appointment_obs.get("valueDatetime")
    return commonutils.validate_date(next_appointment_date) 