from typing import Optional
from datetime import datetime, date

from utils import commonutils


def get_first_obs_with_values(doc, form_id, concept_id, value_coded_arr, earliest_cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    # Normalize the cutoff date once at the start
    target_cutoff = commonutils.normalize_clinical_date(earliest_cutoff_datetime or datetime(1900,1,1))
    
    for obs in obs_list:
        # Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0 ):

            #valueCoded= int(float(obs.get("valueCoded"))) if obs.get("valueCoded") is not None else None
            #if valueCoded not in value_coded_arr:
            #   continue
            
            # Normalize the observation date
            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))      
            
            if isinstance(obs_dt, datetime):
                # Compare two normalized, naive WAT datetimes
                 if obs_dt and target_cutoff and obs_dt >= target_cutoff:
                    matching_obs.append(obs)

    if not matching_obs:
        return None
        
    # 4. Sort using the normalized dates to ensure stability
    matching_obs.sort(
        key=lambda x: commonutils.normalize_clinical_date(x.get('obsDatetime')) or datetime(1900,1,1), 
        reverse=False
    )
    
    return matching_obs[0]
def get_first_obs(doc,form_id,concept_id, earliest_cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0):
            
            # 2. Access the datetime object directly
            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            
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
    matching_obs.sort(key=lambda x: commonutils.normalize_clinical_date(x.get('obsDatetime')) or datetime(1900,1,1))
    
    return matching_obs[0]

def get_last_obs_with_valuecoded_before_date(doc, form_id, concept_id, value_coded_arr, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    cutoff_datetime=commonutils.normalize_clinical_date(cutoff_datetime)

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0 and
            obs.get("valueCoded") in value_coded_arr):
            
            # 2. Access the datetime object directly
            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            
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

    # 1. Standardize the cutoff to be Naive and WAT (+1)
    # If None, it defaults to Now (standardized)
    target_cutoff = commonutils.normalize_clinical_date(cutoff_datetime or datetime.now())
    
    matching_obs = []

    for obs in obs_list:
        # 1. Check basic criteria
        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0):
            
            # 2. Standardize the clinical date from MongoDB
            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            
            # 3. Safe comparison between two Naive WAT datetimes
            if obs_dt and target_cutoff and obs_dt <= target_cutoff:
                matching_obs.append(obs)

    if not matching_obs:
        return None

    # 4. Sort using the normalized dates to ensure stability
    matching_obs.sort(
        key=lambda x: commonutils.normalize_clinical_date(x.get('obsDatetime')) or datetime(1900,1,1), 
        reverse=True
    )
    
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
    matching_obs.sort(key=lambda x: commonutils.normalize_clinical_date(x.get('obsDatetime')) or datetime(1900,1,1), reverse=False)

    # 4. Limit to last x observations
    limited_obs_list = matching_obs[:x]

    # 5. Return the nth item (Index is n-1)
    if len(limited_obs_list) >= n:
        return limited_obs_list[n-1]
    
    return None

def get_nth_obs_of_last_x_obs(doc, form_id, concept_id, n, x, cutoff_datetime: Optional[datetime] = None):
    """
    Retrieves the nth observation from the most recent x observations.
    Example: 3rd viral load of the latest 3.
    """
    observations = doc.get('messageData', {}).get('obs', [])
    filtered_obs = []

    for ob in observations:
        # Match IDs and ensure not voided
        if (ob.get('formId') == form_id and 
            ob.get('conceptId') == concept_id and 
            ob.get('voided') == 0):
            
            # Use the helper logic to get a python datetime
            raw_date = ob.get('obsDatetime')
            obs_dt = None
            if isinstance(raw_date, dict) and '$date' in raw_date:
                obs_dt = datetime.fromisoformat(raw_date['$date'].replace('Z', '+00:00'))
            elif isinstance(raw_date, str):
                obs_dt = datetime.fromisoformat(raw_date.replace('Z', '+00:00'))
            
            if obs_dt:
                if cutoff_datetime:
                    if obs_dt >= cutoff_datetime:
                        filtered_obs.append((obs_dt, ob))
                else:
                    filtered_obs.append((obs_dt, ob))

    if not filtered_obs:
        return None

    # Step 1: Sort by date DESCENDING to get the latest ones first
    filtered_obs.sort(key=lambda item: item[0], reverse=True)

    # Step 2: Take the latest 'x' observations
    latest_x_subset = filtered_obs[:x]

    # Step 3: Sort the subset ASCENDING so n=1 is the oldest of the batch 
    # and n=3 is the newest of the batch.
    latest_x_subset.sort(key=lambda item: item[0])

    # Step 4: Return the nth (n-1 for 0-based indexing)
    try:
        if 0 < n <= len(latest_x_subset):
            return latest_x_subset[n-1][1]
    except (IndexError, TypeError):
        return None

    return None

def get_nth_obs_of_last_x_obs2(doc, form_id, concept_id, n, x, cutoff_datetime: Optional[datetime] = None):
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
    matching_obs_list.sort(key=lambda x: commonutils.normalize_clinical_date(x.get('obsDatetime')) or datetime(1900,1,1))

    # 4. Limit to last x observations
    limited_obs_list = matching_obs_list[:x]

    # 5. Return the nth item (Index is n-1)
    # Check if the list is long enough to avoid IndexError
    if len(limited_obs_list) >= n:
        return limited_obs_list[n-1]
    
    return None

def get_nth_obs(doc, form_id, concept_id, n, cutoff_datetime: Optional[datetime] = None):
    """
    Retrieves the nth observation for a specific form and concept,
    sorted by date, starting from a particular start datetime.
    """
    # 1. Access observations array
    observations = doc.get('messageData', {}).get('obs', [])
    
    filtered_obs = []
    
    for ob in observations:
        # 2. Match form and concept IDs
        if ob.get('formId') == form_id and ob.get('conceptId') == concept_id:
            
            # 3. FIX: Safely extract and parse the datetime
            raw_date = ob.get('obsDatetime')
            obs_dt = None
            
            if isinstance(raw_date, dict) and '$date' in raw_date:
                # Handle MongoDB format: {"$date": "2023-07-12T23:00:00.000Z"}
                date_str = raw_date['$date']
                # Replace 'Z' with +00:00 for fromisoformat compatibility
                obs_dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            elif isinstance(raw_date, datetime):
                # If it's already a datetime (e.g., if using a modern MongoDB driver directly)
                obs_dt = raw_date
            elif isinstance(raw_date, str):
                # If it's stored as a simple string
                obs_dt = datetime.fromisoformat(raw_date.replace('Z', '+00:00'))
            
            # 4. Filter and Append
            if obs_dt:
                if cutoff_datetime:
                    if obs_dt >= cutoff_datetime:
                        filtered_obs.append((obs_dt, ob))
                else:
                    filtered_obs.append((obs_dt, ob))
    
    # 5. Sort by datetime ascending
    filtered_obs.sort(key=lambda x: x[0],reverse=True)
    
    # 6. Return nth result (1-indexed)
    try:
        if 0 < n <= len(filtered_obs):
            return filtered_obs[n-1][1]
    except (IndexError, TypeError):
        return None
    
    return None

def get_nth_obs3(doc, form_id, concept_id, n, cutoff_datetime: Optional[datetime] = None):
    """
    Retrieves the nth observation for a specific form and concept,
    sorted by date, starting from a particular start datetime.
    """
    # 1. Access the list of observations from the patient document
    # Based on the structure: doc['messageData']['obs']
    observations = doc.get('messageData', {}).get('obs', [])
    
    filtered_obs = []
    
    for ob in observations:
        # 2. Check if the observation matches the form and concept IDs
        if ob.get('formId') == form_id and ob.get('conceptId') == concept_id:
            
            # 3. Parse the obsDatetime
            # MongoDB JSON exports often use {'$date': 'ISO-String'}
            raw_date = ob.get('obsDatetime')
            if isinstance(raw_date, dict) and '$date' in raw_date:
                obs_dt = datetime.fromisoformat(raw_date['$date'].replace('Z', '+00:00'))
            else:
                # Fallback if it's already a string or datetime object
                obs_dt = raw_date 
            
            # 4. Apply the cutoff filter (e.g., start of FY25)
            if cutoff_datetime:
                if obs_dt >= cutoff_datetime:
                    filtered_obs.append((obs_dt, ob))
            else:
                filtered_obs.append((obs_dt, ob))
    
    # 5. Sort the matching observations by date (Ascending)
    filtered_obs.sort(key=lambda x: x[0])
    
    # 6. Return the nth element (n-1 because list indexing starts at 0)
    # Returns None if there aren't enough observations
    try:
        if len(filtered_obs) >= n:
            return filtered_obs[n-1][1]
    except (IndexError, TypeError):
        return None
    
    return None

def get_nth_obs2(doc, form_id, concept_id, n, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])

    if not obs_list:
        return None
    
    #target_cutoff = commonutils.normalize_clinical_date(cutoff_datetime or datetime(1900,1,1)) 
    cutoff_datetime = commonutils.normalize_clinical_date(cutoff_datetime or datetime(2024,10,1)) if cutoff_datetime else None

    matching_obs_list = []

    for e in obs_list:
        # 1. Standard clinical checks
        if (e.get("formId") == form_id and 
            e.get("conceptId") == concept_id and 
            e.get("voided") == 0):
            
            obs_dt = commonutils.normalize_clinical_date(e.get('obsDatetime'))
            
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
    matching_obs_list.sort(key=lambda x: commonutils.normalize_clinical_date(x.get('obsDatetime')) or datetime(1900,1,1))

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
            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            if isinstance(obs_dt, datetime):
                matching_obs.append(obs)
                
    if not matching_obs:
        return None
    return matching_obs[0]
            
def get_first_obs_with_value(doc,form_id,concept_id, value_coded_arr, earliest_cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    # FIX 1: Normalize the cutoff date once at the start
    target_cutoff = commonutils.normalize_clinical_date(earliest_cutoff_datetime)
    
    # FIX 2: Ensure value_coded_arr is a list of strings if your JSON stores them as strings
    # Or cast the obs.get("valueCoded") to int below.
    
    for obs in obs_list:
        # Check basic criteria
        # Add int() cast to valueCoded to be safe against string/int mismatches
        raw_val = obs.get("valueCoded")
        try:
            val_as_int = int(float(raw_val)) if raw_val is not None else None
        except (ValueError, TypeError):
            val_as_int = None

        if (obs.get("formId") == form_id and 
            obs.get("conceptId") == concept_id and 
            obs.get("voided") == 0 and
            val_as_int in value_coded_arr):
            
            # Normalize the observation date
            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))      
            
            if isinstance(obs_dt, datetime):
                # FIX 3: Compare two normalized, naive WAT datetimes
                if target_cutoff is None or obs_dt >= target_cutoff:
                    # Store normalized date to avoid re-calculating during sort
                    obs['_normalized_dt'] = obs_dt
                    matching_obs.append(obs)

    if not matching_obs:
        return None
        
    # Sort by the pre-calculated normalized datetime
    matching_obs.sort(key=lambda x: x['_normalized_dt'])
    
    return matching_obs[0]


def getValueDatetimeFromObs(obs):
    if obs is None:
        return None
    return commonutils.normalize_clinical_date(obs.get("valueDatetime"))

def getObsDatetimeFromObs(obs):
    if obs is None:
        return None
    return commonutils.normalize_clinical_date(obs.get("obsDatetime"))

def getValueNumericFromObs(obs):
    if obs is None:
        return None
    value_numeric = obs.get("valueNumeric")
    return float(value_numeric) if value_numeric is not None else None

def getValueCodedFromObs(obs):
    if obs is None:
        return None
    return obs.get("valueCoded")   

def getValueTextFromObs(obs):
    if obs is None:
        return None
    return obs.get("valueText")

def getObsIDFromObs(obs):
    if obs is None:
        return None
    return obs.get("obsId")

def getVariableValueFromObs(obs):
    if obs is None:
        return None
    return obs.get("variableValue")
 # get all obs with concept id in a form with form id, if you find two obs with the same concept id, get the one with the latest datetime.
def getAllObsWithConceptIDRemoveDuplicateByDate(doc, form_id, concept_id, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    obs_dict = {}

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    for obs in obs_list:
        if (obs.get("formId") == form_id and
            obs.get("conceptId") == concept_id and
            obs.get("voided") == 0):

            obs_dt = commonutils.validate_date(obs.get("obsDatetime"))
            if isinstance(obs_dt, datetime):
                existing_obs = obs_dict.get(concept_id)
                if existing_obs is None or obs_dt > commonutils.validate_date(existing_obs.get("obsDatetime")) or obs_dt <= cutoff_datetime: # type: ignore
                    obs_dict[concept_id] = obs

    # sort the obs by datetime in ascending order and return as a list. Oldest first
    sorted_obs = sorted(obs_dict.values(), key=lambda x: commonutils.normalize_clinical_date(x.get("obsDatetime")) or datetime(1900,1,1))
    return sorted_obs

    return list(obs_dict.values())

   
     
def get_first_obs_after_date(doc, form_id, concept_id, cutoff_datetime: Optional[datetime] = None):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()

    for obs in obs_list:
        if (obs.get("formId") == form_id and
            obs.get("conceptId") == concept_id and
            obs.get("voided") == 0):

            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            if isinstance(obs_dt, datetime) and obs_dt > cutoff_datetime:
                matching_obs.append(obs)

    if not matching_obs:
        return None

    # Sort by the actual datetime objects (Oldest first)
    matching_obs.sort(key=lambda x: commonutils.normalize_clinical_date(x.get('obsDatetime')) or datetime(1900,1,1))
    
    return matching_obs[0]    

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

def get_first_obs_between_dates(doc, form_id, concept_id, start_datetime, end_datetime):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    end_datetime=commonutils.normalize_clinical_date(end_datetime)
    start_datetime=commonutils.normalize_clinical_date(start_datetime)  

    for obs in obs_list:
        if (obs.get("formId") == form_id and
            obs.get("conceptId") == concept_id and
            obs.get("voided") == 0):

            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            if isinstance(obs_dt, datetime) and start_datetime and end_datetime:
                if start_datetime <= obs_dt <= end_datetime:
                    matching_obs.append(obs)


    if not matching_obs:
        return None

    # Sort by datetime in ascending order and return the first one
    matching_obs.sort(key=lambda x: commonutils.normalize_clinical_date(x.get("obsDatetime")) or datetime(1900,1,1))
    return matching_obs[0]

def get_first_unsuppressed_viral_load_between_dates(doc, form_id, concept_id, start_datetime, end_datetime, suppression_threshold):
    # 1. Retrieve the obs list safely
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    # 2. Normalize input boundaries
    # Using your commonutils or native datetime if commonutils isn't available
    norm_start = commonutils.normalize_clinical_date(start_datetime)
    norm_end = commonutils.normalize_clinical_date(end_datetime)

    for obs in obs_list:
        # 3. Basic Filter: Form, Concept, and Voided status
        if (obs.get("formId") == form_id and
            obs.get("conceptId") == concept_id and
            obs.get("voided") == 0):

            # 4. Normalize the observation date
            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            
            # 5. Robust numeric conversion
            try:
                raw_val = obs.get("valueNumeric")
                value_numeric = float(raw_val) if raw_val is not None else None
            except (ValueError, TypeError):
                value_numeric = None

            # 6. Apply combined filters
            if isinstance(obs_dt, datetime) and norm_start and norm_end and value_numeric is not None:
                if norm_start <= obs_dt <= norm_end and value_numeric >= suppression_threshold:
                    # Store as a tuple (date, obs) for easier sorting
                    matching_obs.append((obs_dt, obs))

    if not matching_obs:
        return None

    # 7. Sort by the actual datetime objects (index 0 of our tuple)
    # This ensures matching_obs[0] is the chronologically EARLIEST
    matching_obs.sort(key=lambda x: x[0])
    
    # Return the observation part of the first tuple
    return matching_obs[0][1]

def get_first_unsuppressed_viral_load_between_dates2(doc, form_id, concept_id, start_datetime, end_datetime, suppression_threshold):
    obs_list = doc.get("messageData", {}).get("obs", [])
    matching_obs = []

    end_datetime=commonutils.normalize_clinical_date(end_datetime)
    start_datetime=commonutils.normalize_clinical_date(start_datetime)  

    for obs in obs_list:
        if (obs.get("formId") == form_id and
            obs.get("conceptId") == concept_id and
            obs.get("voided") == 0):

            obs_dt = commonutils.normalize_clinical_date(obs.get("obsDatetime"))
            value_numeric = float(obs.get("valueNumeric")) if obs.get("valueNumeric") is not None else None  # convert to float
            if isinstance(obs_dt, datetime) and start_datetime and end_datetime and value_numeric is not None:
                if start_datetime <= obs_dt <= end_datetime and value_numeric >= suppression_threshold:
                    matching_obs.append(obs)


    if not matching_obs:
        return None

    # Sort by datetime in ascending order and return the first one
    matching_obs.sort(key=lambda x: commonutils.normalize_clinical_date(x.get("obsDatetime")) or datetime(1900,1,1))
    return matching_obs[0]