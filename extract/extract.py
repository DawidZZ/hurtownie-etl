import pandas as pd

from transform.transform import transform_damage_property_to_number

DETAILS_FILE_PATH = 'data/details.csv'
DENSITY_FILE_PATH = 'data/density.csv'

storm_details_dtype = {
    'YEAR': 'int64',
    'MONTH_NAME': 'str',
    'BEGIN_YEARMONTH': 'int64',
    'BEGIN_DAY': 'int64',
    'END_YEARMONTH': 'int64',
    'END_DAY': 'int64',
    'STATE': 'str',
    'CZ_NAME': 'str',
    'BEGIN_LAT': 'float64',
    'BEGIN_LON': 'float64',
    'SOURCE': 'str',
    'FLOOD_CAUSE': 'str',
    'EVENT_TYPE': 'str',
    'WFO': 'str',
    'INJURIES_DIRECT': 'int64',
    'INJURIES_INDIRECT': 'int64',
    'DEATHS_DIRECT': 'int64',
    'DEATHS_INDIRECT': 'int64',
    'MAGNITUDE': 'float64',
    'MAGNITUDE_TYPE': 'str',
}

storm_details_converters = {
    'DAMAGE_PROPERTY': transform_damage_property_to_number
}

population_density_dtype = {
    'state': 'str',
    'year': 'int64',
    'population_density': 'float64'
}

def extract_and_clean_storm_details_data(columns=None):
    df = pd.read_csv(DETAILS_FILE_PATH, engine="python", usecols=columns, dtype=storm_details_dtype, converters=storm_details_converters, on_bad_lines=lambda x: print(x))
    df.drop_duplicates(inplace=True)
    return df


def extract_and_clean_density_data(columns=None):
    df = pd.read_csv(DENSITY_FILE_PATH, engine="python", usecols=columns, dtype=population_density_dtype, on_bad_lines=lambda x: print(x))
    df.drop_duplicates(inplace=True)
    return df
