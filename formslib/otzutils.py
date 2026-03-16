
from typing import Final, Optional
from datetime import datetime, date
from typing import Optional

from utils import obsutils
import utils.encounterutils as encounterutils

OTZ_FORM_ID = 73
OTZ_OUTCOME_DATE_CONCEPT_ID = 166008

def get_otz_enrollment_date(doc, cutoff_datetime: Optional[datetime] = None):
    otz_enrollment_encounter = encounterutils.get_last_encounter_by_form_id(doc, OTZ_FORM_ID, cutoff_datetime)
    if otz_enrollment_encounter:
        return encounterutils.get_encounter_datetime(otz_enrollment_encounter)
    return None
def get_otz_outcome_date(doc, cutoff_datetime: Optional[datetime] = None):
    otz_outcome_date_obs =obsutils.get_last_obs_before_date(doc, OTZ_OUTCOME_DATE_CONCEPT_ID, cutoff_datetime)
    if otz_outcome_date_obs:
        return obsutils.getValueDatetimeFromObs(otz_outcome_date_obs)
    return None