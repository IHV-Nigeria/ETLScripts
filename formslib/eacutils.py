

from datetime import datetime, date
from typing import Optional
from utils.encounterutils import get_last_encounter_by_form_id, get_nth_encounter
from utils.obsutils import get_obs_with_encounter_id


#Constants for EAC
EAC_FORM_ID = 69
EAC_SESSION_TYPE_CONCEPT_ID = 166097
EAC_SESSION_TYPE_CONCEPT_ID = 166097
EAC_BARRIERS_TO_ADHERENCE_CONCEPT_ID = 165457
EAC_REGIMEN_PLAN_CONCEPT_ID = 165771
EAC_FOLLOWUP_DATE_CONCEPT_ID = 165036
EAC_ADHERENCE_COMMENTS_CONCEPT_ID = 165606


def get_eac_date(n, doc):
    eacn = get_nth_encounter(doc, EAC_FORM_ID, n)
    eacn_date = eacn.get("encounterDatetime") if eacn else None
    return eacn_date

def get_nth_eac_encounter(doc,n):
    eacn = get_nth_encounter(doc, EAC_FORM_ID, n)
    return eacn

def get_last_eac_session_type(doc,last_eac_encounter, cutoff_datetime: Optional[datetime] = None):
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_session_type_obs = get_obs_with_encounter_id(doc, EAC_SESSION_TYPE_CONCEPT_ID, last_eac_encounter_id)
    eac_session_type = last_eac_session_type_obs.get("variableValue") if last_eac_session_type_obs else None
    return eac_session_type

def get_last_eac_comments(doc, last_eac_encounter, cutoff_datetime: Optional[datetime] = None):
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_comments_obs = get_obs_with_encounter_id(doc, EAC_ADHERENCE_COMMENTS_CONCEPT_ID, last_eac_encounter_id)
    eac_comments = last_eac_comments_obs.get("valueText") if last_eac_comments_obs else None
    return eac_comments

def get_last_eac_session_type_datetime(doc, last_eac_encounter, cutoff_datetime: Optional[datetime] = None):
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_session_type_obs = get_obs_with_encounter_id(doc, EAC_SESSION_TYPE_CONCEPT_ID, last_eac_encounter_id)
    eac_session_type_datetime = last_eac_session_type_obs.get("obsDatetime") if last_eac_session_type_obs else None
    return eac_session_type_datetime

def get_last_eac_encounter(doc, cutoff_datetime: Optional[datetime] = None):
    last_eac_encounter = get_last_encounter_by_form_id(doc,EAC_FORM_ID, cutoff_datetime)
    return last_eac_encounter

def get_last_eac_barriers_to_adherence(doc, last_eac_encounter, cutoff_datetime: Optional[datetime] = None):
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_barriers_to_adherence_obs = get_obs_with_encounter_id(doc, EAC_BARRIERS_TO_ADHERENCE_CONCEPT_ID, last_eac_encounter_id)
    eac_barriers_to_adherence = last_eac_barriers_to_adherence_obs.get("variableValue") if last_eac_barriers_to_adherence_obs else None
    return eac_barriers_to_adherence

def get_last_eac_regimen_plan(doc, last_eac_encounter, cutoff_datetime: Optional[datetime] = None):
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_regimen_plan_obs = get_obs_with_encounter_id(doc, EAC_REGIMEN_PLAN_CONCEPT_ID, last_eac_encounter_id)
    eac_regimen_plan = last_eac_regimen_plan_obs.get("variableValue") if last_eac_regimen_plan_obs else None
    return eac_regimen_plan
def get_last_eac_followup_date(doc, last_eac_encounter, cutoff_datetime: Optional[datetime] = None):
    if not last_eac_encounter:
        return None
    last_eac_encounter_id = last_eac_encounter.get("encounterId")
    last_eac_followup_date_obs = get_obs_with_encounter_id(doc, EAC_FOLLOWUP_DATE_CONCEPT_ID, last_eac_encounter_id)
    eac_followup_date = last_eac_followup_date_obs.get("valueDatetime") if last_eac_followup_date_obs else None
    return eac_followup_date