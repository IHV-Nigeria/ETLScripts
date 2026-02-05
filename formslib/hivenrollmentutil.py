
from typing import Final, Optional
from datetime import datetime, date
import utils.obsutils as obsutils
import formslib.ctdutils as ctdutils
import utils.commonutils as commonutils
import pandas as pd

# Constants for HIV Enrollment
HIV_ENROLLMENT_FORM_ID = 23

ART_START_DATE_CONCEPT_ID = 159599
CARE_ENTRY_POINT_CONCEPT_ID = 160540
KP_TYPE_CONCEPT_ID = 166369
DATE_TRANSFERED_IN_CONCEPT_ID = 160534
PRIOR_ART_CONCEPT_ID = 165242



def get_date_transferred_in(doc,cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, HIV_ENROLLMENT_FORM_ID, DATE_TRANSFERED_IN_CONCEPT_ID,cutoff_datetime)
    # Result if true | condition | result if false
    date_transferred_in = obs.get("valueDatetime") if obs else None
    return commonutils.validate_date(date_transferred_in)
    

def get_care_entry_point(doc,cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, HIV_ENROLLMENT_FORM_ID, CARE_ENTRY_POINT_CONCEPT_ID,cutoff_datetime)
    care_entry_point = obs.get("variableValue") if obs else None
    return care_entry_point

def get_prior_art(doc,cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, HIV_ENROLLMENT_FORM_ID, PRIOR_ART_CONCEPT_ID,cutoff_datetime)
    prior_art = obs.get("variableValue") if obs else None
    return prior_art
    
def get_kp_type(doc,cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, HIV_ENROLLMENT_FORM_ID, KP_TYPE_CONCEPT_ID,cutoff_datetime)
    kp_type = obs.get("variableValue") if obs else None
    return kp_type