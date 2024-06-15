import numpy as np
import pandas as pd

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


def insert_population_density_to_main_dataset(data, population_density_data):
    population_density_data = population_density_data.drop("Unnamed: 0", axis='columns')
    population_density_data['state'] = population_density_data['state'].apply(str.upper)
    population_density_dict = population_density_data.set_index(['state', 'year']).to_dict(orient='index')
    data['population_density'] = data.apply(
        transform_lookup_population_density(population_density_dict), axis=1)
    return data


def transform_columns_data(data):
    data['DAMAGE_PROPERTY'] = data['DAMAGE_PROPERTY'].apply(transform_damage_property_to_number)
    data['BEGIN_YEARMONTH'] = data['BEGIN_YEARMONTH'].apply(transform_get_month_from_yearmonth)
    return data


def create_derived_columns(data):
    data['magnitude_group'] = data['MAGNITUDE'].apply(create_group_magnitude)
    data['damage_group'] = data['DAMAGE_PROPERTY'].apply(create_group_damage_property)
    data['injuries_total'] = data['INJURIES_DIRECT'] + data['INJURIES_INDIRECT']
    data['deaths_total'] = data['DEATHS_DIRECT'] + data['DEATHS_INDIRECT']
    return data


def drop_unused_columns(data, required_columns):
    return data.loc[:, required_columns]
    