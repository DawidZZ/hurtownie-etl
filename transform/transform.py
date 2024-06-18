import math
import numpy as np
import pandas as pd
from datetime import datetime

VALID_US_STATE_NAMES = [
    'ALABAMA', 'ALASKA', 'ARIZONA', 'ARKANSAS', 'CALIFORNIA', 'COLORADO', 'CONNECTICUT', 'DELAWARE', 'FLORIDA',
    'GEORGIA', 'HAWAII', 'IDAHO', 'ILLINOIS', 'INDIANA', 'IOWA', 'KANSAS', 'KENTUCKY', 'LOUISIANA', 'MAINE',
    'MARYLAND', 'MASSACHUSETTS', 'MICHIGAN', 'MINNESOTA', 'MISSISSIPPI', 'MISSOURI', 'MONTANA', 'NEBRASKA',
    'NEVADA', 'NEW HAMPSHIRE', 'NEW JERSEY', 'NEW MEXICO', 'NEW YORK', 'NORTH CAROLINA', 'NORTH DAKOTA', 'OHIO',
    'OKLAHOMA', 'OREGON', 'PENNSYLVANIA', 'RHODE ISLAND', 'SOUTH CAROLINA', 'SOUTH DAKOTA', 'TENNESSEE', 'TEXAS',
    'UTAH', 'VERMONT', 'VIRGINIA', 'WASHINGTON', 'WEST VIRGINIA', 'WISCONSIN', 'WYOMING'
]

VALID_MONTH_NAMES = [
    'January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November',
]


def create_group_magnitude(data):
    magnitude = data['MAGNITUDE']

    if pd.isnull(magnitude):
        return 'nie dotyczy'
    elif data["MAGNITUDE"] != math.floor(data["MAGNITUDE"]):
        return 'grad'
    else:
        # skala Beauforta (węzły)
        if magnitude <= 1:
            return 'Cisza'
        elif 1 < magnitude <= 3:
            return 'Powiew'
        elif 3 < magnitude <= 6:
            return 'Słaby wiatr'
        elif 6 < magnitude <= 10:
            return 'Łagodny wiatr'
        elif 10 < magnitude <= 16:
            return 'Umiarkowany wiatr'
        elif 16 < magnitude <= 21:
            return 'Dość silny wiatr'
        elif 21 < magnitude <= 27:
            return 'Silny wiatr'
        elif 27 < magnitude <= 33:
            return 'Bardzo silny wiatr'
        elif 33 < magnitude <= 40:
            return 'Sztorm'
        elif 40 < magnitude <= 47:
            return 'Silny sztorm'
        elif 47 < magnitude <= 55:
            return 'Bardzo silny sztorm'
        elif 55 < magnitude <= 63:
            return 'Gwałtowny sztorm'
        else:
            return 'Huragan'


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
    yearmonth = int(yearmonth)
    day = int(day)
    year = yearmonth // 100
    month = yearmonth % 100
    return datetime(year, month, day)


def create_duration(row):
    if pd.isnull(row['BEGIN_YEARMONTH']) or pd.isna(row['BEGIN_YEARMONTH']) or pd.isnull(
            row['END_YEARMONTH']) or pd.isna(row['END_YEARMONTH']):
        return None
    begin_date = to_datetime(row['BEGIN_YEARMONTH'], row['BEGIN_DAY'])
    end_date = to_datetime(row['END_YEARMONTH'], row['END_DAY'])
    if end_date < begin_date:
        return None
    return (end_date - begin_date).days + 1


def transform_lookup_population_density(population_density_dict):
    def transform(row):
        if pd.isnull(row['YEAR']) or pd.isna(row['YEAR']):
            return None
        year = find_nearest(range(1990, 2020, 10), row['YEAR'])
        density_dict = population_density_dict.get((row['STATE'], year), None)
        if density_dict is not None:
            return density_dict.get('density', None)
        else:
            return None

    return transform


def transform_get_month_from_yearmonth(yearmonth):
    return int(yearmonth) % 100


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
    population_density_data.dropna(inplace=True)
    population_density_dict = population_density_data.set_index(
        ['state', 'year']).to_dict(orient='index')
    data['population_density'] = data.apply(
        transform_lookup_population_density(population_density_dict), axis=1)
    return data


def create_derived_columns(data):
    data['magnitude_group'] = data.apply(create_group_magnitude, axis=1)
    data['damage_group'] = data['DAMAGE_PROPERTY'].apply(
        create_group_damage_property)
    data['duration'] = data.apply(create_duration, axis=1)
    data['injuries_total'] = data['INJURIES_DIRECT'] + data['INJURIES_INDIRECT']
    data['deaths_total'] = data['DEATHS_DIRECT'] + data['DEATHS_INDIRECT']
    data['MONTH'] = data['BEGIN_YEARMONTH'].apply(
        transform_get_month_from_yearmonth)
    data['QUARTER'] = ((data['MONTH'] - 1) // 3) + 1

    return data


def drop_unused_columns(data, required_columns):
    return data.loc[:, required_columns]


# def drop_invalid_rows(data):
#     data = data[data['MONTH_NAME'].str.capitalize().isin(VALID_MONTH_NAMES)]
#     return data[data['STATE'].str.upper().isin(VALID_US_STATE_NAMES)]


def fill_missing_values(data):
    data['BEGIN_LAT'] = data.apply(fill_lat(data), axis=1)
    data['BEGIN_LON'] = data.apply(fill_lon(data), axis=1)
    return data


def drop_rows_with_missing_values(data, columns):
    return data.dropna(subset=columns)
