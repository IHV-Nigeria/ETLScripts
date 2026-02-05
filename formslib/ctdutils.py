from typing import Optional
from datetime import datetime, date
from utils import commonutils
import utils.obsutils as obsutils



# Constants for Client Tracking
CLIENT_TRACKING_DISCONTINUATION_FORM_ID = 13
REASON_FOR_TERMINATION_CONCEPT_ID = 165470

def get_patient_outcome(doc, cutoff_datetime: Optional[datetime] = None):
    reson_for_termination_obs = obsutils.get_last_obs_before_date(doc, CLIENT_TRACKING_DISCONTINUATION_FORM_ID, REASON_FOR_TERMINATION_CONCEPT_ID, cutoff_datetime)
    if not reson_for_termination_obs:
        return None
    termination_reason = reson_for_termination_obs.get("variableValue") 
    return termination_reason

def get_outcome_date(doc, cutoff_datetime: Optional[datetime] = None):
    reson_for_termination_obs = obsutils.get_last_obs_before_date(doc, CLIENT_TRACKING_DISCONTINUATION_FORM_ID, REASON_FOR_TERMINATION_CONCEPT_ID, cutoff_datetime)
    if not reson_for_termination_obs:
        return None
    termination_date = reson_for_termination_obs.get("obsDatetime") 
    return commonutils.validate_date(termination_date)