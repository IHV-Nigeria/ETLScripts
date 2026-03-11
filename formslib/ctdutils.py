from typing import Final, Optional
from datetime import datetime, date
from utils import commonutils
import utils.obsutils as obsutils



# Constants for Client Tracking
CLIENT_TRACKING_DISCONTINUATION_FORM_ID = 13
REASON_FOR_TERMINATION_CONCEPT_ID = 165470
DATE_RETURNED_TO_CARE: Final[int] = 165775
DATE_OF_TERMINATION: Final[int] = 165469


def get_patient_outcome(doc, cutoff_datetime: Optional[datetime] = None):
    reson_for_termination_obs = obsutils.get_last_obs_before_date(doc, CLIENT_TRACKING_DISCONTINUATION_FORM_ID, REASON_FOR_TERMINATION_CONCEPT_ID, cutoff_datetime)
    if not reson_for_termination_obs:
        return None
    termination_reason = reson_for_termination_obs.get("variableValue") 
    return termination_reason

def get_date_returned_to_care(doc, cutoff_datetime: Optional[datetime] = None):
    date_returned_to_care_obs = obsutils.get_last_obs_before_date(doc, CLIENT_TRACKING_DISCONTINUATION_FORM_ID, DATE_RETURNED_TO_CARE, cutoff_datetime)
    if not date_returned_to_care_obs:
        return None
    date_returned_to_care = obsutils.getValueDatetimeFromObs(date_returned_to_care_obs)
    return date_returned_to_care

def get_date_of_termination(doc, cutoff_datetime: Optional[datetime] = None):
    date_of_termination_obs = obsutils.get_last_obs_before_date(doc, CLIENT_TRACKING_DISCONTINUATION_FORM_ID, DATE_OF_TERMINATION, cutoff_datetime)
    if not date_of_termination_obs:
        return None
    date_of_termination = obsutils.getValueDatetimeFromObs(date_of_termination_obs)
    return date_of_termination

def get_outcome_date(doc, cutoff_datetime: Optional[datetime] = None):
    reson_for_termination_obs = obsutils.get_last_obs_before_date(doc, CLIENT_TRACKING_DISCONTINUATION_FORM_ID, REASON_FOR_TERMINATION_CONCEPT_ID, cutoff_datetime)
    if not reson_for_termination_obs:
        return None
    termination_date = reson_for_termination_obs.get("obsDatetime") 
    return commonutils.validate_date(termination_date)