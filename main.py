import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, Float, String, ForeignKey
import pandas as pd

# Load environment variables
load_dotenv()

# Get database name from environment variables
db_name = os.getenv("DB_NAME")

# Create a connection string
CONNECTION_STRING = f"mssql+pyodbc://@{db_name}/hurtownie_projekt?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

# Create a SQLAlchemy engine
engine = create_engine(CONNECTION_STRING)

# Define functions to load and clean data
def load_and_clean_data(file_path):
    df = pd.read_csv(file_path)
    df.drop_duplicates(inplace=True)
    return df


def map_damage_property(damage_property: object):
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


def magnitude_group(magnitude):
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


def damage_group(damage_property):
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


# Load data
storm_details_data = load_and_clean_data('data/details.csv')
population_density_data = load_and_clean_data('data/density.csv')

# Map DAMAGE_PROPERTY to float
storm_details_data['DAMAGE_PROPERTY'] = storm_details_data['DAMAGE_PROPERTY'].apply(map_damage_property)

# Apply grouping functions
storm_details_data['magnitude_group'] = storm_details_data['MAGNITUDE'].apply(magnitude_group)
storm_details_data['damage_group'] = storm_details_data['DAMAGE_PROPERTY'].apply(damage_group)

# Ensure all necessary columns are included
required_columns = [
    'YEAR', 'BEGIN_YEARMONTH', 'BEGIN_DAY', 'STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON',
    'SOURCE', 'FLOOD_CAUSE', 'EVENT_TYPE', 'WFO', 'INJURIES_DIRECT', 'INJURIES_INDIRECT',
    'DEATHS_DIRECT', 'DEATHS_INDIRECT', 'MAGNITUDE', 'magnitude_group', 'damage_group', 'DAMAGE_PROPERTY'
]

# Filter columns to match the database schema using .loc to avoid copy issues
storm_details_filtered = storm_details_data.loc[:, required_columns]

# Create additional required columns
storm_details_filtered['injuries_total'] = storm_details_filtered['INJURIES_DIRECT'] + storm_details_filtered[
    'INJURIES_INDIRECT']
storm_details_filtered['deaths_total'] = storm_details_filtered['DEATHS_DIRECT'] + storm_details_filtered[
    'DEATHS_INDIRECT']

# Prepare population density mapping
population_density_dict = population_density_data.set_index(['state', 'year'])['density'].to_dict()


def get_population_density(row):
    return population_density_dict.get((row['STATE'], row['YEAR']), None)


storm_details_filtered['population_density'] = storm_details_filtered.apply(get_population_density, axis=1)

# # Drop rows with missing required data
# storm_details_filtered.dropna(subset=[
#     'YEAR', 'BEGIN_YEARMONTH', 'BEGIN_DAY', 'STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON',
#     'SOURCE', 'FLOOD_CAUSE', 'EVENT_TYPE', 'WFO', 'population_density'], inplace=True)

# Check for final columns in storm_details_filtered
print(storm_details_filtered.head())

# Create required tables
metadata = MetaData()

dim_time = Table('dim_time', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('year', Integer),
                 Column('month', Integer),
                 Column('day', Integer),
                 Column('quarter', Integer)
                 )

dim_location = Table('dim_location', metadata,
                     Column('id', Integer, primary_key=True),
                     Column('state', String(255)),
                     Column('cz_name', String(255)),
                     Column('lat', Float),
                     Column('lon', Float)
                     )

dim_source = Table('dim_source', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('source', String(255))
                   )

dim_flood_cause = Table('dim_flood_cause', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('flood_cause', String(255))
                        )

dim_event_type = Table('dim_event_type', metadata,
                       Column('id', Integer, primary_key=True),
                       Column('event_type', String(255))
                       )

dim_wfo = Table('dim_wfo', metadata,
                Column('id', Integer, primary_key=True),
                Column('wfo', String(10))
                )

dim_population_density = Table('dim_population_density', metadata,
                               Column('id', Integer, primary_key=True),
                               Column('density', Float)
                               )

fact_event = Table('fact_event', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('time_id', Integer, ForeignKey('dim_time.id')),
                   Column('location_id', Integer, ForeignKey('dim_location.id')),
                   Column('source_id', Integer, ForeignKey('dim_source.id')),
                   Column('flood_cause_id', Integer, ForeignKey('dim_flood_cause.id')),
                   Column('event_type_id', Integer, ForeignKey('dim_event_type.id')),
                   Column('wfo_id', Integer, ForeignKey('dim_wfo.id')),
                   Column('population_density_id', Integer, ForeignKey('dim_population_density.id')),
                   Column('injuries_direct', Integer),
                   Column('injuries_indirect', Integer),
                   Column('injuries_total', Integer),
                   Column('deaths_direct', Integer),
                   Column('deaths_indirect', Integer),
                   Column('deaths_total', Integer),
                   Column('magnitude', Float),
                   Column('magnitude_group', String(100)),
                   Column('damage_group', String(100)),
                   Column('duration', Integer),
                   Column('damage_property', Float)
                   )

metadata.create_all(engine)

# Load data into database
with engine.connect() as conn:
    # Insert into dimension tables
    # time_data = storm_details_filtered[['YEAR', 'BEGIN_YEARMONTH', 'BEGIN_DAY']].drop_duplicates()
    # time_data.columns = ['year', 'month', 'day']
    # time_data['quarter'] = ((time_data['month'] - 1) // 3) + 1
    # time_data.to_sql('dim_time', conn, if_exists='append', index=False)

    # location_data = storm_details_filtered[['STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON']].drop_duplicates()
    # location_data.columns = ['state', 'cz_name', 'lat', 'lon']
    # location_data.to_sql('dim_location', conn, if_exists='append', index=False)

    source_data = storm_details_filtered[['SOURCE']].drop_duplicates()
    source_data.columns = ['source']
    source_data.to_sql('dim_source', conn, if_exists='append', index=False)

    # flood_cause_data = storm_details_filtered[['FLOOD_CAUSE']].drop_duplicates()
    # flood_cause_data.columns = ['flood_cause']
    # flood_cause_data.to_sql('dim_flood_cause', conn, if_exists='append', index=False)

    # event_type_data = storm_details_filtered[['EVENT_TYPE']].drop_duplicates()
    # event_type_data.columns = ['event_type']
    # event_type_data.to_sql('dim_event_type', conn, if_exists='append', index=False)

    # wfo_data = storm_details_filtered[['WFO']].drop_duplicates()
    # wfo_data.columns = ['wfo']
    # wfo_data.to_sql('dim_wfo', conn, if_exists='append', index=False)

    # population_density_data = storm_details_filtered[['population_density']].drop_duplicates()
    # population_density_data.columns = ['density']
    # population_density_data.to_sql('dim_population_density', conn, if_exists='append', index=False)

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
