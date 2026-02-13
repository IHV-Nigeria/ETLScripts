
from typing import Optional
from datetime import datetime, date

import formslib.ctdutils as ctdutils
from utils import commonutils


def get_next_pickup_date_from_encounterlist(encounter_list, pickup_date: datetime):
    future_pickup_dates = []

    for encounter in encounter_list:
        if encounter.get("voided") == 0:
            encounter_datetime = encounter.get("encounterDatetime")
            if isinstance(encounter_datetime, datetime):
                if encounter_datetime > pickup_date:
                    future_pickup_dates.append(encounter_datetime)

    if not future_pickup_dates:
        return None

    # Sort the dates to find the earliest one
    future_pickup_dates.sort()
    return future_pickup_dates[0]



def get_all_encounters_by_form_id(doc, form_id, cutoff_datetime: Optional[datetime] = None):
    encounter_list = doc.get("messageData", {}).get("encounters", [])
    matching_encounters = []

    # If a cutoff date is not provided use current date as cutoff
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    for encounter in encounter_list:
        if (encounter.get("formId") == form_id and
            encounter.get("voided") ==0):

            encounter_datetime = encounter.get("encounterDatetime")

            if isinstance(encounter_datetime, datetime):
                if encounter_datetime <= cutoff_datetime:
                    matching_encounters.append(encounter)

    if not matching_encounters:
        return None

    # 3. Sort by the actual datetime objects (Newest first)
    matching_encounters.sort(key=lambda x: x.get('encounterDatetime'), reverse=False)
    
    return matching_encounters


def get_last_encounter_by_form_id(doc, form_id, cutoff_datetime: Optional[datetime] = None):
    encounter_list = doc.get("messageData", {}).get("encounters", [])
    matching_encounters = []

    # If a cutoff date is not provided use current date as cutoff
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    for encounter in encounter_list:
        if (encounter.get("formId") == form_id and
            encounter.get("voided") ==0):

            encounter_datetime = encounter.get("encounterDatetime")

            if isinstance(encounter_datetime, datetime):
                if encounter_datetime <= cutoff_datetime:
                    matching_encounters.append(encounter)

    if not matching_encounters:
        return None

    # 3. Sort by the actual datetime objects (Newest first)
    matching_encounters.sort(key=lambda x: x.get('encounterDatetime'), reverse=True)
    
    return matching_encounters[0]



def get_last_encounter_date(doc, cutoff_datetime: Optional[datetime] = None):
    encounter = get_last_encounter(doc, cutoff_datetime) 
    encounter_datetime = encounter.get('encounterDatetime') if encounter else None
    return commonutils.validate_date(encounter_datetime)

def get_nth_encounter_after_date(doc, form_id, n, after_date):
    if after_date is None:
        return None

    encounter_list = doc.get("messageData", {}).get("encounters", [])
    matching_encounters = []

    for encounter in encounter_list:
        if (encounter.get("formId") == form_id and
            encounter.get("voided") == 0):

            encounter_datetime = encounter.get("encounterDatetime")

            if isinstance(encounter_datetime, datetime):
                if encounter_datetime > after_date:
                    matching_encounters.append(encounter)

    if not matching_encounters:
        return None

    # Sort by the actual datetime objects (Oldest first)
    matching_encounters.sort(key=lambda x: x.get('encounterDatetime'))
    
    # Return the nth item (Index is n-1)
    if len(matching_encounters) >= n:
        return matching_encounters[n-1]
    
    return None

def get_nth_encounter(doc, form_id, n):
    """
    Retrieves the nth occurrence of a specific form based on encounterDatetime.
    
    Args:
        doc (dict): The NMRS patient document.
        form_id (int): The ID of the form (e.g., 69 for EAC).
        n (int): The position (1 for first, 2 for second, etc.).
        
    Returns:
        dict: The nth encounter object, or None if it doesn't exist.
    """
    # 1. Access the encounters array
    encounters = doc.get("messageData", {}).get("encounters", [])
    
    # 2. Filter for matching formId and ensure not voided
    matching_encounters = [
        e for e in encounters 
        if e.get("formId") == form_id and e.get("voided") == 0
    ]
    
    if not matching_encounters:
        return None

    # 3. Sort by encounterDatetime (Oldest to Newest)
    # Using .get() for the sort key handles potential missing dates
    matching_encounters.sort(key=lambda x: x.get('encounterDatetime', datetime.min))
    
    # 4. Return the nth item (Index is n-1)
    # Check if the list is long enough to avoid IndexError
    if len(matching_encounters) >= n:
        return matching_encounters[n-1]
    
    return None
def get_encounter_datetime(encounter):
    if encounter is None:
        return None
    encounter_datetime = encounter.get('encounterDatetime')
    
    return commonutils.validate_date(encounter_datetime)

def get_encounter_id(encounter):
    if encounter is None:
        return None
    
    encounter_id = encounter.get('encounterId')
    return encounter_id

def get_last_encounter(doc,cutoff_datetime: Optional[datetime] = None):
    encounter_list = doc.get("messageData", {}).get("encounters", [])
    matching_encounters = []

    # If a cutoff date is not provided use current date as cutoff
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    for encounter in encounter_list:
        if (encounter.get("formId") != ctdutils.CLIENT_TRACKING_DISCONTINUATION_FORM_ID and
            encounter.get("voided") ==0):

            encounter_datetime = encounter.get("encounterDatetime")

            if isinstance(encounter_datetime, datetime):
                if encounter_datetime <= cutoff_datetime:
                    matching_encounters.append(encounter)

    if not matching_encounters:
        return None

    # 3. Sort by the actual datetime objects (Newest first)
    matching_encounters.sort(key=lambda x: x.get('encounterDatetime'), reverse=True)
    
    return matching_encounters[0]
                