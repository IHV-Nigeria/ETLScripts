from typing import Final, Optional
from datetime import datetime, date
from formslib.eacutils import get_eac_date
import utils.obsutils as obsutils
import formslib.ctdutils as ctdutils
import pandas as pd

# Constants for Lab forms
LAB_FORM_ID: Final[int] = 21
VIRAL_LOAD_CONCEPT_ID: Final[int] = 856
SAMPLE_COLLECTION_DATE_CONCEPT_ID: Final[int] = 159951
VIRAL_LOAD_REPORTED_DATE_CONCEPT_ID: Final[int] = 165414
VIRAL_LOAD_INDICATION_CONCEPT_ID: Final[int] = 164980
VIRAL_LOAD_ORDERED_DATE_CONCEPT_ID: Final[int] = 164989



def get_nth_viral_load_obs(doc, n , cutoff_datetime: Optional[datetime] = None):
    nth_viral_load_obs = obsutils.get_nth_obs(doc, LAB_FORM_ID, VIRAL_LOAD_CONCEPT_ID, n, cutoff_datetime)
    return nth_viral_load_obs


def get_nth_viral_sample_collection_obs(doc, nth_viral_load_obs):
    if nth_viral_load_obs is None:
        return None
        
    encounter_id = nth_viral_load_obs.get('encounterId')
    sample_collection_obs = obsutils.get_obs_with_encounter_id(doc, SAMPLE_COLLECTION_DATE_CONCEPT_ID, encounter_id)
    return sample_collection_obs

def get_nth_viral_load_reported_obs(doc, n , nth_viral_load_obs):
    if nth_viral_load_obs is None:
        return None
    
    encounter_id = nth_viral_load_obs.get('encounterId') 
    
    reported_date_obs = obsutils.get_obs_with_encounter_id(doc, VIRAL_LOAD_REPORTED_DATE_CONCEPT_ID, encounter_id)
    
    return reported_date_obs

def get_nth_viral_load_obs_of_last_x_viral_loads(doc, n, x, cutoff_datetime: Optional[datetime] = None):
    viral_load_obs = obsutils.get_nth_obs_of_last_x_obs(doc, LAB_FORM_ID, VIRAL_LOAD_CONCEPT_ID, n, x, cutoff_datetime)

    return viral_load_obs
   

    
def get_sample_collection_date_obs_of_viral_load_obs(doc, viral_load_obs):
    if not viral_load_obs:
        return None
    encounter_id = viral_load_obs.get('encounterId')
    sample_collection_date_obs =    obsutils.get_obs_with_encounter_id(doc, SAMPLE_COLLECTION_DATE_CONCEPT_ID, encounter_id)
    

    return sample_collection_date_obs

def get_reported_date_obs_of_viral_load_obs(doc, viral_load_obs):
    if not viral_load_obs:
        return None
    encounter_id = viral_load_obs.get('encounterId')
    reported_date_obs = obsutils.get_obs_with_encounter_id(doc, VIRAL_LOAD_REPORTED_DATE_CONCEPT_ID, encounter_id)
    
    return reported_date_obs


def get_viral_load_indication_obs_of_viral_load_obs(doc, viral_load_obs):
    if not viral_load_obs:
        return None
    encounter_id = viral_load_obs.get('encounterId')
    viral_load_indication_obs = obsutils.get_obs_with_encounter_id(doc, VIRAL_LOAD_INDICATION_CONCEPT_ID, encounter_id)
    
    return viral_load_indication_obs

def get_last_sample_taken_date_obs(doc, cutoff_datetime: Optional[datetime] = None):
    viral_load_sample_obs = obsutils.get_last_obs_before_date(doc, LAB_FORM_ID, SAMPLE_COLLECTION_DATE_CONCEPT_ID, cutoff_datetime)
    if not viral_load_sample_obs:
        return None
    return viral_load_sample_obs


    
    
def get_last_viral_load_obs_before(doc, cutoff_datetime):
    viral_load_obs = obsutils.get_last_obs_before_date(doc, LAB_FORM_ID, VIRAL_LOAD_CONCEPT_ID , cutoff_datetime)
    return viral_load_obs
