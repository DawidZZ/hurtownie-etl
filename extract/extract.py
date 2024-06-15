import pandas as pd

DETAILS_FILE_PATH = 'data/details.csv'
DENSITY_FILE_PATH = 'data/density.csv'
def load_and_clean_storm_details_data():
    df = pd.read_csv(DETAILS_FILE_PATH)
    df.drop_duplicates(inplace=True)
    return df

def load_and_clean_density_data():
    df = pd.read_csv(DENSITY_FILE_PATH)
    df.drop_duplicates(inplace=True)
    return df