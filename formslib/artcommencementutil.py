from typing import Final, Optional
from datetime import datetime, date
import utils.obsutils as obsutils
import pandas as pd




# Constants for Art Commencement
ART_COMMENCEMENT_FORM_ID = 56
ART_START_DATE_CONCEPT_ID = 159599
REGIMEN_LINE_AT_ART_START_CONCEPT_ID = 165708
CHILD_2ND_LINE_REGIMEN_CONCEPT_ID = 164514
ADULT_2ND_LINE_REGIMEN_CONCEPT_ID = 164513
ADULT_3RD_LINE_REGIMEN_CONCEPT_ID = 165702
CHILD_3RD_LINE_REGIMEN_CONCEPT_ID = 165703
ADULT_1ST_LINE_REGIMEN_CONCEPT_ID = 164506
CHILD_FIRST_LINE_REGIMEN_CONCEPT_ID = 164507
WHO_STAGE_AT_ART_START_CONCEPT_ID = 5356
PREGNANCY_STATUS_AT_ART_START_CONCEPT_ID = 165050
CD4_COUNT_AT_ART_START_CONCEPT_ID = 164429


def get_art_start_date(doc,cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, ART_COMMENCEMENT_FORM_ID, ART_START_DATE_CONCEPT_ID,cutoff_datetime)
    # Result if true | condition | result if false
    art_start_date = obs.get("valueDatetime") if obs else None
    return art_start_date

def get_who_stage_at_art_start(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, ART_COMMENCEMENT_FORM_ID, WHO_STAGE_AT_ART_START_CONCEPT_ID, cutoff_datetime)
    who_stage_at_art_start = obs.get("variableValue") if obs else None
    return who_stage_at_art_start

def get_pregnancy_status_at_art_start(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, ART_COMMENCEMENT_FORM_ID, PREGNANCY_STATUS_AT_ART_START_CONCEPT_ID, cutoff_datetime)
    pregnancy_status_at_art_start = obs.get("variableValue") if obs else None
    return pregnancy_status_at_art_start

def get_cd4_count_at_art_start(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, ART_COMMENCEMENT_FORM_ID, CD4_COUNT_AT_ART_START_CONCEPT_ID, cutoff_datetime)
    cd4_count_at_art_start = obs.get("valueNumeric") if obs else None
    return cd4_count_at_art_start

def get_current_regimen(doc, cutoff_datetime: Optional[datetime] = None):
    current_regimen_line_obs = obsutils.get_last_obs_before_date(doc, ART_COMMENCEMENT_FORM_ID, REGIMEN_LINE_AT_ART_START_CONCEPT_ID, cutoff_datetime)
    if(current_regimen_line_obs is None):
        return None
    valueCoded = current_regimen_line_obs.get("valueCoded") 
    encounter_id = current_regimen_line_obs.get("encounterId")
    current_regimen_obs = obsutils.get_obs_with_encounter_id(doc, valueCoded, encounter_id)
    current_regimen = current_regimen_obs.get("variableValue") if current_regimen_obs else None
    return current_regimen

def get_current_regimen_line(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, ART_COMMENCEMENT_FORM_ID, REGIMEN_LINE_AT_ART_START_CONCEPT_ID, cutoff_datetime)
    current_regimen_line = obs.get("variableValue") if obs else None
    return current_regimen_line

