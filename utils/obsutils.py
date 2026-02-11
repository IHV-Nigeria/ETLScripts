from typing import Optional
from datetime import datetime, date

from utils import commonutils


def get_first_obs(doc,form_id,concept_id, earliest_cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0):
            
            # 2. Access the datetime object directly
            obs_dt = obs.get("obsDatetime")
            
            # Ensure it is a valid datetime object before comparing
            if isinstance(obs_dt, datetime):
                # 2. Check the optional earliest cutoff
                # If None, the condition is always True. 
                # If provided, obs_dt must be >= cutoff.
                if earliest_cutoff_datetime is None or obs_dt >= earliest_cutoff_datetime:
                    matching_obs.append(obs)
                

    if not matching_obs:
        return None
        
    # 3. Sort by the actual datetime objects (Oldest first)
    matching_obs.sort(key=lambda x: x.get('obsDatetime'))
    
    return matching_obs[0]

def get_last_obs_with_valuecoded_before_date(doc, form_id, concept_id, value_coded_arr, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0 and
            obs.get("valueCoded") in value_coded_arr):
            
            # 2. Access the datetime object directly
            obs_dt = obs.get("obsDatetime")
            
            # Ensure it is a valid datetime object before comparing
            if isinstance(obs_dt, datetime):
                if cutoff_datetime is None or obs_dt <= cutoff_datetime:
                    matching_obs.append(obs)

    if not matching_obs:
        return None

    # 3. Sort by the actual datetime objects (Newest first)
    matching_obs.sort(key=lambda x: x.get('obsDatetime'), reverse=True)
    
    return matching_obs[0]

def get_last_obs_before_date(doc, form_id, concept_id, cutoff_datetime: Optional[datetime] = None):
    """
    Finds the most recent non-voided observation for a specific form and concept
    that occurred on or before the cutoff date.
    
    Args:
        doc (dict): The patient JSON document.
        form_id (int): The specific formId (e.g., 14 for Care Card).
        concept_id (int): The specific conceptId (e.g., 5089 for Weight).
        cutoff_date (datetime): The Python datetime object for the cutoff.
        
    Returns:
        dict: The most recent observation object, or None if not found.
    """
    obs_list = doc.get("messageData", {}).get("obs", [])

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0):
            
            # 2. Access the datetime object directly
            obs_dt = obs.get("obsDatetime")
            
            # Ensure it is a valid datetime object before comparing
            if isinstance(obs_dt, datetime):
                if obs_dt <= cutoff_datetime:
                    matching_obs.append(obs)

    if not matching_obs:
        return None

    # 3. Sort by the actual datetime objects (Newest first)
    matching_obs.sort(key=lambda x: x['obsDatetime'], reverse=True)
    
    return matching_obs[0]
def get_nth_obs_of_last_x_obs_with_valuecoded(doc, form_id, concept_id,  value_coded_arr, n, x, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0 and
            obs.get("valueCoded") in value_coded_arr):
            
            # 2. Access the datetime object directly
            obs_dt = obs.get("obsDatetime")
            
            # Ensure it is a valid datetime object before comparing
            if isinstance(obs_dt, datetime):
                if obs_dt <= cutoff_datetime:
                    matching_obs.append(obs)

    if not matching_obs:
        return None

    # 3. Sort by the actual datetime objects (Oldest first)
    matching_obs.sort(key=lambda x: x['obsDatetime'], reverse=False)

    # 4. Limit to last x observations
    limited_obs_list = matching_obs[:x]

    # 5. Return the nth item (Index is n-1)
    if len(limited_obs_list) >= n:
        return limited_obs_list[n-1]
    
    return None
def get_nth_obs_of_last_x_obs(doc, form_id, concept_id, n, x, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])

    if not obs_list:
        return None

    matching_obs_list = []

    for e in obs_list:
        # 1. Standard clinical checks
        if (e.get("formId") == form_id and 
            e.get("conceptId") == concept_id and 
            e.get("voided") == 0):
            
            obs_dt = e.get('obsDatetime')
            
            # 2. Check if the date exists and handle the optional cutoff
            if isinstance(obs_dt, datetime):
                # If cutoff is None, we accept all dates. 
                # If cutoff is provided, we check if obs_dt is on or after it.
                if cutoff_datetime is None or obs_dt >= cutoff_datetime:
                    matching_obs_list.append(e)
   
    if not matching_obs_list:
        return None

    # 3. Sort by encounterDatetime (Oldest to Newest)
    # Using .get() for the sort key handles potential missing dates
    matching_obs_list.sort(key=lambda x: x.get('obsDatetime'))

    # 4. Limit to last x observations
    limited_obs_list = matching_obs_list[:x]

    # 5. Return the nth item (Index is n-1)
    # Check if the list is long enough to avoid IndexError
    if len(limited_obs_list) >= n:
        return limited_obs_list[n-1]
    
    return None

def get_nth_obs(doc, form_id, concept_id, n, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])

    if not obs_list:
        return None

    matching_obs_list = []

    for e in obs_list:
        # 1. Standard clinical checks
        if (e.get("formId") == form_id and 
            e.get("conceptId") == concept_id and 
            e.get("voided") == 0):
            
            obs_dt = e.get('obsDatetime')
            
            # 2. Check if the date exists and handle the optional cutoff
            if isinstance(obs_dt, datetime):
                # If cutoff is None, we accept all dates. 
                # If cutoff is provided, we check if obs_dt is on or after it.
                if cutoff_datetime is None or obs_dt >= cutoff_datetime:
                    matching_obs_list.append(e)
   
    if not matching_obs_list:
        return None

    # 3. Sort by encounterDatetime (Oldest to Newest)
    # Using .get() for the sort key handles potential missing dates
    matching_obs_list.sort(key=lambda x: x.get('obsDatetime'))

    # 4. Return the nth item (Index is n-1)
    # Check if the list is long enough to avoid IndexError
    if len(matching_obs_list) >= n:
        return matching_obs_list[n-1]
    
    return None

def get_obs_with_group_id(doc, form_id, encounter_id, search_obs_concept_id,obs_group_id):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        if (obs.get("formId") == form_id and
            obs.get("encounterId") == encounter_id and
            obs.get("conceptId") == search_obs_concept_id and
            obs.get("obsGroupId") == obs_group_id and
            obs.get("voided") == 0 ):

            # 2. Access the datetime object directly
            obs_dt = commonutils.validate_date(obs.get("obsDatetime"))
            if isinstance(obs_dt, datetime):
                matching_obs.append(obs)
                
    if not matching_obs:
        return None
    return matching_obs[0]
            
def get_first_obs_with_value(doc,form_id,concept_id, value_coded_arr, earliest_cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0 and
            obs.get("valueCoded") in value_coded_arr):
            
            # 2. Access the datetime object directly
            obs_dt = commonutils.validate_date(obs.get("obsDatetime"))      
            
            # Ensure it is a valid datetime object before comparing
            if isinstance(obs_dt, datetime):
                # 2. Check the optional earliest cutoff
                # If None, the condition is always True. 
                # If provided, obs_dt must be >= cutoff.
                if earliest_cutoff_datetime is None or obs_dt >= earliest_cutoff_datetime:
                    matching_obs.append(obs)
                

    if not matching_obs:
        return None
        
    # 3. Sort by the actual datetime objects (Oldest first)
    matching_obs.sort(key=lambda x: x.get('obsDatetime'))
    
    return matching_obs[0] 

def getValueDatetimeFromObs(obs):
    if obs is None:
        return None
    return commonutils.validate_date(obs.get("valueDatetime"))

def getObsDatetimeFromObs(obs):
    if obs is None:
        return None
    return commonutils.validate_date(obs.get("obsDatetime"))

def getValueNumericFromObs(obs):
    if obs is None:
        return None
    return obs.get("valueNumeric")

def getValueCodedFromObs(obs):
    if obs is None:
        return None
    return obs.get("valueCoded")   

def getValueTextFromObs(obs):
    if obs is None:
        return None
    return obs.get("valueText")

def getVariableValueFromObs(obs):
    if obs is None:
        return None
    return obs.get("variableValue")
 



def get_obs_with_encounter_id(doc, concept_id, encounter_id):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        if (obs.get("encounterId") == encounter_id and
            obs.get("conceptId") == concept_id and
            obs.get("voided") == 0 ):

            matching_obs.append(obs)
            
    if not matching_obs:
        return None
        
    return matching_obs[0]


   
