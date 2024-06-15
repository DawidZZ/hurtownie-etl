import numpy as np
import pandas as pd
from datetime import datetime

def transform_damage_property_to_number(damage_property: object):
    damage_property = str(damage_property)
    if pd.isnull(damage_property):
        return 0
    elif 'K' in damage_property:
        damage_property = damage_property[:-1].strip()
        return float(damage_property) * 1000 if damage_property.isdigit() else 0.0
    elif 'M' in damage_property:
        damage_property = damage_property[:-1].strip()
        return float(damage_property) * 1000000 if damage_property.isdigit() else 0.0
    else:
        damage_property = damage_property[:-1].strip()
        return float(damage_property) if damage_property.isdigit() else 0.0


def create_group_magnitude(magnitude):
    if pd.isnull(magnitude) or magnitude == 0.0 or type(magnitude) is str:
        return 'brak'
    elif magnitude < 1.0:
        return 'słaby'
    elif 1.0 <= magnitude < 3.0:
        return 'średni'
    elif 3.0 <= magnitude < 5.0:
        return 'silny'
    else:
        return 'ekstremalny'


def create_group_damage_property(damage_property):
    if pd.isnull(damage_property) or damage_property == 0:
        return 'brak'
    if damage_property < 1000:
        return 'niskie'
    elif 1000 <= damage_property < 10000:
        return 'umiarkowane'
    elif 10000 <= damage_property < 100000:
        return 'wysokie'
    else:
        return 'ekstremalne'

def to_datetime(yearmonth, day):
    year = yearmonth // 100
    month = yearmonth % 100
    return datetime(year, month, day)

def create_duration(row):
    begin_date = to_datetime(row['BEGIN_YEARMONTH'], row['BEGIN_DAY'])
    end_date = to_datetime(row['END_YEARMONTH'], row['END_DAY'])
    return (end_date - begin_date).days

def transform_lookup_population_density(population_density_dict):
    def transform(row):
        year = find_nearest(range(1990, 2020, 10), row['YEAR'])
        density_dict = population_density_dict.get((row['STATE'], year), None)
        if density_dict is not None:
            return density_dict.get('density',None)
        else:
            return None
    return transform

def transform_get_month_from_yearmonth(yearmonth):
    return int(str(yearmonth)[-2:])

def find_nearest(array, value):
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return array[idx]


def fill_lat(data):
    county_lat = data.groupby('CZ_NAME')['BEGIN_LAT'].first().to_dict()
    state_lat = data.groupby('STATE')['BEGIN_LAT'].first().to_dict()
    
    def get_lat(row):
        if pd.notnull(row['BEGIN_LAT']):
            return row['BEGIN_LAT']
        elif row['CZ_NAME'] in county_lat and pd.notnull(county_lat[row['CZ_NAME']]):
            return county_lat[row['CZ_NAME']]
        elif row['STATE'] in state_lat and pd.notnull(state_lat[row['STATE']]):
            return state_lat[row['STATE']]
        else:
            return np.nan
    
    return get_lat

def fill_lon(data):
    county_lon = data.groupby('CZ_NAME')['BEGIN_LON'].first().to_dict()
    state_lon = data.groupby('STATE')['BEGIN_LON'].first().to_dict()
    
    def get_lon(row):
        if pd.notnull(row['BEGIN_LON']):
            return row['BEGIN_LON']
        elif row['CZ_NAME'] in county_lon and pd.notnull(county_lon[row['CZ_NAME']]):
            return county_lon[row['CZ_NAME']]
        elif row['STATE'] in state_lon and pd.notnull(state_lon[row['STATE']]):
            return state_lon[row['STATE']]
        else:
            return np.nan
    
    return get_lon


def insert_population_density_to_main_dataset(data, population_density_data):
    population_density_data = population_density_data.drop("Unnamed: 0", axis='columns')
    population_density_data['state'] = population_density_data['state'].apply(str.upper)
    population_density_dict = population_density_data.set_index(['state', 'year']).to_dict(orient='index')
    data['population_density'] = data.apply(
        transform_lookup_population_density(population_density_dict), axis=1)
    return data


def transform_columns_data(data):
    data['DAMAGE_PROPERTY'] = data['DAMAGE_PROPERTY'].apply(transform_damage_property_to_number)
    return data


def create_derived_columns(data):
    data['magnitude_group'] = data['MAGNITUDE'].apply(create_group_magnitude)
    data['damage_group'] = data['DAMAGE_PROPERTY'].apply(create_group_damage_property)
    data['duration'] = data.apply(create_duration, axis=1)
    data['injuries_total'] = data['INJURIES_DIRECT'] + data['INJURIES_INDIRECT']
    data['deaths_total'] = data['DEATHS_DIRECT'] + data['DEATHS_INDIRECT']
    data['MONTH'] = data['BEGIN_YEARMONTH'].apply(transform_get_month_from_yearmonth)
    
    return data


def drop_unused_columns(data, required_columns):
    return data.loc[:, required_columns]
    

def fill_missing_values(data):
    data['BEGIN_LAT'] = data.apply(fill_lat(data), axis=1)
    data['BEGIN_LON'] = data.apply(fill_lon(data), axis=1)
    return data