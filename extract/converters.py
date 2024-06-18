import numpy as np
import pandas as pd

def validate_int(x):
    try:
        value = int(x)
        if value < 0:
            return np.nan
        return value
    except (ValueError, TypeError):
        return np.nan

def validate_year(x):
    try:
        value = int(x)
        if 1000 <= value <= 9999:
            return value
        return np.nan
    except (ValueError, TypeError):
        return np.nan

def validate_state(x):
    valid_states = [
        "ALABAMA", "ALASKA", "ARIZONA", "ARKANSAS", "CALIFORNIA", "COLORADO", "CONNECTICUT", "DELAWARE",
        "FLORIDA", "GEORGIA", "HAWAII", "IDAHO", "ILLINOIS", "INDIANA", "IOWA", "KANSAS", "KENTUCKY",
        "LOUISIANA", "MAINE", "MARYLAND", "MASSACHUSETTS", "MICHIGAN", "MINNESOTA", "MISSISSIPPI", "MISSOURI",
        "MONTANA", "NEBRASKA", "NEVADA", "NEW HAMPSHIRE", "NEW JERSEY", "NEW MEXICO", "NEW YORK", "NORTH CAROLINA",
        "NORTH DAKOTA", "OHIO", "OKLAHOMA", "OREGON", "PENNSYLVANIA", "RHODE ISLAND", "SOUTH CAROLINA",
        "SOUTH DAKOTA", "TENNESSEE", "TEXAS", "UTAH", "VERMONT", "VIRGINIA", "WASHINGTON", "WEST VIRGINIA",
        "WISCONSIN", "WYOMING"
    ]
    if isinstance(x, str):
        x = x.upper()
        return x if x in valid_states else np.nan
    return np.nan

def validate_month_name(x):
    valid_months = {
        "January", "February", "March", "April", "May", "June", 
        "July", "August", "September", "October", "November", "December"
    }
    if isinstance(x, str):
        x = x.capitalize()
        return x if x in valid_months else np.nan
    return np.nan

def validate_yearmonth(x):
    try:
        x_str = str(x)
        if len(x_str) == 6 and x_str.isdigit():
            return int(x)
        return np.nan
    except (ValueError, TypeError):
        return np.nan

def validate_day(x):
    try:
        value = int(x)
        if 1 <= value <= 31:
            return value
        return np.nan
    except (ValueError, TypeError):
        return np.nan

def validate_float(x):
    try:
        value = float(x)
        return value
    except (ValueError, TypeError):
        return np.nan

def validate_greater_than_0_float(x):
    try:
        value = float(x)
        return value if value > 0 else np.nan
    except (ValueError, TypeError):
        return np.nan

def transform_damage_property_to_number(x):
    if pd.isnull(x):
        return 0.0
    if not isinstance(x, str):
        return 0.0
    
    x = x.strip().upper()
    
    try:
        if x.endswith('K'):
            return float(x[:-1]) * 1000
        elif x.endswith('M'):
            return float(x[:-1]) * 1000000
        else:
            return float(x)
    except ValueError:
        return 0.0

storm_details_converters = {
    'YEAR': validate_year,
    'MONTH_NAME': validate_month_name,
    'BEGIN_YEARMONTH': validate_yearmonth,
    'BEGIN_DAY': validate_day,
    'END_YEARMONTH': validate_yearmonth,
    'END_DAY': validate_day,
    'BEGIN_LAT': validate_float,
    'BEGIN_LON': validate_float,
    'FLOOD_CAUSE': lambda x: x if isinstance(x, str) else np.nan,
    'EVENT_TYPE': lambda x: x if isinstance(x, str) else np.nan,
    'WFO': lambda x: x if isinstance(x, str) else np.nan,
    'INJURIES_DIRECT': validate_int,
    'INJURIES_INDIRECT': validate_int,
    'DEATHS_DIRECT': validate_int,
    'DEATHS_INDIRECT': validate_int,
    'MAGNITUDE': validate_greater_than_0_float,
    'MAGNITUDE_TYPE': lambda x: x if isinstance(x, str) else np.nan,
    'DAMAGE_PROPERTY': transform_damage_property_to_number,
    'SOURCE': lambda x: x.lower().strip() if isinstance(x, str) else np.nan,
    'CZ_NAME': lambda x: x.lower().strip() if isinstance(x, str) else np.nan,
    'STATE': validate_state,
}

population_density_converters = {
    'state': validate_state,
    'year': validate_year,
    'population_density': validate_float
}