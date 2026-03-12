from typing import Optional
from datetime import datetime, date
from utils import commonutils
import utils.obsutils as obsutils

from utils.obsutils import get_first_obs

# Constants on Care Card
IPT_FIELD_FORM_ID = 53

# IPT Concept IDs

INH_START_DATE_CONCEPT_ID = 165994
INH_OUTCOME_CONCEPT_ID = 166007
INH_OUTCOME_DATE_CONCEPT_ID = 166008

def get_inh_start_date(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, IPT_FIELD_FORM_ID, INH_START_DATE_CONCEPT_ID, cutoff_datetime)
    inh_start_date = obsutils.getValueDatetimeFromObs(obs) if obs else None
    return inh_start_date   
def get_inh_outcome(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, IPT_FIELD_FORM_ID, INH_OUTCOME_CONCEPT_ID, cutoff_datetime)
    inh_outcome = obsutils.getVariableValueFromObs(obs) if obs else None
    return inh_outcome
def get_inh_outcome_date(doc, cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, IPT_FIELD_FORM_ID, INH_OUTCOME_DATE_CONCEPT_ID, cutoff_datetime)
    inh_outcome_date = obsutils.getValueDatetimeFromObs(obs) if obs else None
    return inh_outcome_date




