
from datetime import datetime, date
from typing import Optional

from utils.commonutils import get_month_diff, get_year_diff

def get_message_header(doc):
    message_header = doc.get('messageHeader', {})
    return message_header


def get_patient_demographics(doc):
    patient_demographics = doc.get('messageData',{}).get('demographics',{})
    return patient_demographics

def get_patient_identifier(identifier_type_id, doc):
    patient_identifiers = doc.get('messageData',{}).get('patientIdentifiers',{})

    # 2. Check if identifiers is actually a list and not empty
    if not isinstance(patient_identifiers, list) or len(patient_identifiers) == 0:
        return None
    # 3. Search for the specific type that is NOT voided
    for item in patient_identifiers:
        if item.get("identifierType") == identifier_type_id and item.get("voided") == 0:
            return item.get("identifier")

    # 4. Return None if the loop finishes without finding a match
    return None

def get_patient_birthdate(doc):
    patient_demographics = get_patient_demographics(doc)
    return patient_demographics.get('birthdate')

def calculateAge(birthdate: datetime):
    
     if birthdate is None:
        return None

     today = date.today()
     dob = birthdate.date()

     return today.year - dob.year - (
        (today.month, today.day) < (dob.month, dob.day)
     )
        
def get_current_age_at_date_in_months(doc, at_date: Optional[datetime] = None):
    birthdate = get_patient_birthdate(doc)
    if at_date is None:
        at_date = datetime.now()
    
    if birthdate is None:
        return None

    today = at_date.date()
    dob = birthdate.date()

    years_difference = today.year - dob.year
    months_difference = today.month - dob.month
    total_months = years_difference * 12 + months_difference

    # Adjust if the current day is before the birth day in the month
    if today.day < dob.day:
        total_months -= 1
    if total_months > 60:
        return None

    return total_months

def get_current_age_at_date(doc, at_date: Optional[datetime] = None):
    birthdate = get_patient_birthdate(doc)
    if at_date is None:
        at_date = datetime.now()
    
    if birthdate is None:
        return None

    today = at_date.date()
    dob = birthdate.date()

    return today.year - dob.year - (
        (today.month, today.day) < (dob.month, dob.day)
    )

def get_patient_current_age(doc):
    birthdate = get_patient_birthdate(doc)
    return calculateAge(birthdate)


def get_age_art_start_months(doc,birthdate, art_start_date):
    if art_start_date is None:
        return None
    if birthdate is None:
        return None
    
    if all([art_start_date, birthdate]):
        age_months = get_month_diff(birthdate, art_start_date)
    else:
        return None
        
    return age_months
            
def get_age_art_start_years(doc,birthdate, art_start_date):
    if art_start_date is None:
        return None
    if birthdate is None:
        return None
    
    if all([art_start_date, birthdate]):
        age_years = get_year_diff(birthdate, art_start_date)
   
    return age_years

def get_pediatric_age_art_start_months(doc, birthdate,art_start_date):

    age_months =  get_age_art_start_months(doc,birthdate, art_start_date)
    if age_months is None:
        return None
    
    if age_months > 60:
        return None
    return age_months

def get_months_on_art(doc, art_start_date, cutoff_datetime: Optional[datetime] = None):
    if art_start_date is None:
        return None
    
    if cutoff_datetime is None:
        cutoff_datetime = datetime.now()
    months_on_art=get_month_diff(art_start_date, cutoff_datetime)
    return months_on_art
    
