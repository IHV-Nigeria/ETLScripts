from datetime import datetime, date
from typing import Optional




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
            return date_val
        else:
            # Return 1960 for typos like 2023702
            return PLACEHOLDER_DATE
    except (AttributeError, ValueError):
        # Handles cases where the object isn't a proper datetime
        return PLACEHOLDER_DATE


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


    
    