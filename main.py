import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

from extract.extract import extract_and_clean_storm_details_data, extract_and_clean_density_data
from load.load import load_data_to_temp_dim_tables, load_data_to_temp_fact_table
from transform.transform import fill_missing_values, insert_population_density_to_main_dataset, drop_unused_columns, transform_columns_data, create_derived_columns
from load.database import create_tmp_tables, drop_tmp_tables

load_dotenv()
db_name = os.getenv("DB_NAME")
CONNECTION_STRING = f"mssql+pyodbc://@{db_name}/hurtownie_projekt?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"


allowed_columns = [
    'YEAR', 'MONTH', 'MONTH_NAME', 'BEGIN_YEARMONTH', 'BEGIN_DAY', 'END_YEARMONTH', 'END_DAY', 'STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON',
    'SOURCE', 'FLOOD_CAUSE', 'EVENT_TYPE', 'WFO', 'INJURIES_DIRECT', 'INJURIES_INDIRECT',
    'DEATHS_DIRECT', 'DEATHS_INDIRECT', 'MAGNITUDE', 'injuries_total', 'deaths_total', 'magnitude_group', 
    'damage_group', 'DAMAGE_PROPERTY', 'population_density', 'duration'
]


def main():
    engine = create_engine(CONNECTION_STRING)

    storm_details_data = extract_and_clean_storm_details_data()
    population_density_data = extract_and_clean_density_data()

    data = insert_population_density_to_main_dataset(storm_details_data, population_density_data)
    data = transform_columns_data(data)
    data = create_derived_columns(data)
    data = fill_missing_values(data)
    data = drop_unused_columns(data, allowed_columns)
    
    print(data.head())

    drop_tmp_tables(engine)
    create_tmp_tables(engine)
    load_data_to_temp_dim_tables(engine, data, 'append')
    load_data_to_temp_fact_table(engine, data)


if __name__ == "__main__":
    main()

# Load data into database


# # Insert into fact table
# fact_event_data = storm_details_filtered[[
#     'INJURIES_DIRECT', 'INJURIES_INDIRECT', 'injuries_total', 'DEATHS_DIRECT', 'DEATHS_INDIRECT', 'deaths_total',
#     'MAGNITUDE', 'magnitude_group', 'damage_group', 'DAMAGE_PROPERTY'
# ]]
# fact_event_data.columns = [
#     'injuries_direct', 'injuries_indirect', 'injuries_total', 'deaths_direct', 'deaths_indirect', 'deaths_total',
#     'magnitude', 'magnitude_group', 'damage_group', 'damage_property'
# ]
# # Add foreign keys
# fact_event_data['time_id'] = time_data.index + 1
# fact_event_data['location_id'] = location_data.index + 1
# fact_event_data['source_id'] = source_data.index + 1
# fact_event_data['flood_cause_id'] = flood_cause_data.index + 1
# fact_event_data['event_type_id'] = event_type_data.index + 1
# fact_event_data['wfo_id'] = wfo_data.index + 1
# fact_event_data['population_density_id'] = population_density_data.index + 1

# fact_event_data.to_sql('fact_event', conn, if_exists='append', index=False)
