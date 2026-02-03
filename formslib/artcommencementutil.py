from typing import Final, Optional
from datetime import datetime, date
import utils.obsutils as obsutils
import pandas as pd




# Constants for Art Commencement
ART_COMMENCEMENT_FORM_ID = 56
ART_START_DATE_CONCEPT_ID = 159599


def get_art_start_date(doc,cutoff_datetime: Optional[datetime] = None):
    obs = obsutils.get_last_obs_before_date(doc, ART_COMMENCEMENT_FORM_ID, ART_START_DATE_CONCEPT_ID,cutoff_datetime)
    # Result if true | condition | result if false
    art_start_date = obs.get("valueDatetime") if obs else None
    return art_start_date

