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


def transform_group_magnitude(magnitude):
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


def transform_group_damage_property(damage_property):
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