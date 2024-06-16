import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

from extract.extract import extract_and_clean_storm_details_data, extract_and_clean_density_data
from load.database import create_tables, create_tmp_tables, drop_tables, drop_tmp_tables
from load.load import load_data_to_destination_tables, load_data_to_dim_tables, load_data_to_temp_fact_table
from transform.transform import drop_invalid_rows, drop_rows_with_missing_values, fill_missing_values, insert_population_density_to_main_dataset, drop_unused_columns, create_derived_columns
import pandas as pd

load_dotenv()
db_name = os.getenv("DB_NAME")
CONNECTION_STRING = f"mssql+pyodbc://@{db_name}/hurtownie_projekt?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

allowed_source_columns_storm = ['YEAR', 'MONTH_NAME', 'BEGIN_YEARMONTH', 'BEGIN_DAY', 'END_YEARMONTH', 'END_DAY', 'STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON',
                                'SOURCE', 'FLOOD_CAUSE', 'EVENT_TYPE', 'WFO', 'INJURIES_DIRECT', 'INJURIES_INDIRECT', 'DEATHS_DIRECT', 'DEATHS_INDIRECT', 'MAGNITUDE', 'MAGNITUDE_TYPE', 'DAMAGE_PROPERTY']

allowed_source_columns_density = ['state', 'year', 'density']

allowed_columns = [
    'YEAR', 'QUARTER', 'MONTH', 'MONTH_NAME', 'BEGIN_DAY',
      'STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON',
    'SOURCE', 'FLOOD_CAUSE', 'EVENT_TYPE', 'WFO', 'INJURIES_DIRECT', 'INJURIES_INDIRECT',
    'DEATHS_DIRECT', 'DEATHS_INDIRECT', 'MAGNITUDE', 'injuries_total', 'deaths_total', 'magnitude_group',
    'damage_group', 'DAMAGE_PROPERTY', 'population_density', 'duration'
]

required_columns = [
    'YEAR', 'QUARTER', 'MONTH', 'MONTH_NAME',  'BEGIN_DAY',
    'STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON', 'EVENT_TYPE', 'WFO', 'INJURIES_DIRECT',
    'INJURIES_INDIRECT', 'DEATHS_DIRECT', 'DEATHS_INDIRECT', 'injuries_total', 'deaths_total', 'magnitude_group', 'damage_group', 'DAMAGE_PROPERTY', 'duration'
]


def null_ratios(df):
    null_counts = df.isnull().sum()
    total_counts = len(df)
    null_percentages = (null_counts / total_counts) * 100
    null_ratios_df = pd.DataFrame({
        'null_count': null_counts,
        'null_percentage': null_percentages
    })
    return null_ratios_df


def main():
    engine = create_engine(CONNECTION_STRING, fast_executemany=True)

    storm_details_data = extract_and_clean_storm_details_data(
        allowed_source_columns_storm)
    population_density_data = extract_and_clean_density_data(
        allowed_source_columns_density)

    data = insert_population_density_to_main_dataset(
        storm_details_data, population_density_data)
    data = create_derived_columns(data)
    data = fill_missing_values(data)
    data = drop_invalid_rows(data)
    data = drop_unused_columns(data, allowed_columns)
    data = drop_rows_with_missing_values(data, required_columns)

    print(data.head())
    print(data.shape)
    # print(null_ratios(data))

    drop_tmp_tables(engine)
    create_tmp_tables(engine)
    load_data_to_dim_tables(engine, data, 'append')
    load_data_to_temp_fact_table(engine, data, 'append')
    
    # drop_tables(engine)
    create_tables(engine)
    load_data_to_destination_tables(engine)


if __name__ == "__main__":
    main()
