import pandas as pd
from extract.converters import storm_details_converters
from extract.converters import population_density_converters

# DETAILS_FILE_PATH = 'data/details.csv'
# DENSITY_FILE_PATH = 'data/density.csv'

DETAILS_FILE_PATH = 'data/details.csv'
DENSITY_FILE_PATH = 'data/density.csv'



def extract_and_clean_storm_details_data(columns=None):
    df = pd.read_csv(DETAILS_FILE_PATH, engine="python", usecols=columns, converters=storm_details_converters, on_bad_lines='skip')
    df.drop_duplicates(inplace=True)
    return df


def extract_and_clean_density_data(columns=None):
    df = pd.read_csv(DENSITY_FILE_PATH, engine="python", usecols=columns, converters=population_density_converters, on_bad_lines='skip')
    df.drop_duplicates(inplace=True)
    return df
