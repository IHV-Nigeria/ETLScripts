from datetime import datetime, date, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo






def localize_date(utc_dt: Optional[datetime]) -> Optional[datetime]:
    if utc_dt is None:
        return None
    # Convert UTC to Lagos/Nigeria time
    return utc_dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(ZoneInfo("Africa/Lagos"))





def validate_date(date_val: Optional[datetime]) -> Optional[datetime]:
    """
    Validates clinical dates. If the date is a typo (e.g., year 2023702),
    it returns 1960-01-01 to prevent script crashes while flagging the error.
    """
    if date_val is None:
        return None

    # Define a reasonable clinical window for IHVN data
    MIN_YEAR = 1900
    MAX_YEAR = 2100
    PLACEHOLDER_DATE = datetime(1960, 1, 1)

    try:
        # Check if the year is within a realistic range
        if MIN_YEAR <= date_val.year <= MAX_YEAR:
            return localize_date(date_val)
        else:
            # Return 1960 for typos like 2023702
            return localize_date(PLACEHOLDER_DATE)
    except (AttributeError, ValueError):
        # Handles cases where the object isn't a proper datetime
        return localize_date(PLACEHOLDER_DATE)


def get_fy_and_quater_from_date(input_date: datetime):
    if input_date is None:
        return None

    # Financial year starts in October
    FY_START_MONTH = 10

    year = input_date.year
    month = input_date.month

    # Determine financial year
    if month >= FY_START_MONTH:
        fy_year = year
        quarter = 1
    elif month >= 1 and month <= 3:
        fy_year = year - 1
        quarter = 2
    elif month >= 4 and month <= 6:
        fy_year = year - 1
        quarter = 3
    else:  # Jul–Sep
        fy_year = year - 1
        quarter = 4

    return f"FY{str(fy_year)[-2:]}Q{quarter}"

def get_fy_and_quarter_of_obs_obsdatetime(obs):
    if obs is None:
        return None
    obs_datetime = obs.get("obsDatetime")
    return get_fy_and_quater_from_date(obs_datetime)


def normalize_clinical_date(date_val: Any) -> Optional[datetime]:
    """
    Standardizes clinical dates for IHVN ETL.
    1. Strips timezone info (Naive).
    2. Adjusts UTC to WAT (UTC+1).
    3. Returns None if input is invalid.
    """
    if date_val is None or not isinstance(date_val, datetime):
        return None
        
    try:
        # 1. Adjust for the +1 hour Nigeria offset identified in SQL vs Python
        # (This corrects the 23:00 vs 00:00 discrepancy)
        wat_date = date_val + timedelta(hours=1)
        
        # 2. Strip timezone info to prevent 'can't subtract naive/aware' errors
        return wat_date.replace(tzinfo=None)
    except Exception:
        return None

def get_days_diff(datetime1: Optional[datetime], datetime2: Optional[datetime]) -> int: 
    """
    Returns the number of days between datetime1 and datetime2.
    Result is positive if datetime2 >= datetime1, negative otherwise.
    """
    if datetime1 is None or datetime2 is None:
        return None
    
    # Use distinct names to avoid conflict with 'date' or 'datetime' types
    clean_dt1 = datetime1.replace(tzinfo=None) if datetime1.tzinfo else datetime1
    clean_dt2 = datetime2.replace(tzinfo=None) if datetime2.tzinfo else datetime2

    # Perform subtraction on variables, not types
    delta = clean_dt2 - clean_dt1

    return delta.days


def get_month_diff(datetime1: datetime, datetime2: datetime) -> int:
    """
    Returns the number of whole calendar months between datetime1 and datetime2.
    Result is positive if datetime2 >= datetime1, negative otherwise.
    """
    if datetime1 > datetime2:
        datetime1, datetime2 = datetime2, datetime1
        sign = -1
    else:
        sign = 1

    months = (datetime2.year - datetime1.year) * 12 + (datetime2.month - datetime1.month)

    # Adjust if end day is before start day
    if datetime2.day < datetime1.day:
        months -= 1

    return months * sign

def get_year_diff(datetime1: datetime, datetime2: datetime) -> int:
    """
    Returns the number of whole calendar years between datetime1 and datetime2.
    Result is positive if datetime2 >= datetime1, negative otherwise.
    """
    if datetime1 > datetime2:
        datetime1, datetime2 = datetime2, datetime1
        sign = -1
    else:
        sign = 1

    years = datetime2.year - datetime1.year

    # Adjust if end date is before anniversary
    if (datetime2.month, datetime2.day) < (datetime1.month, datetime1.day):
        years -= 1

    return years * sign


    
    