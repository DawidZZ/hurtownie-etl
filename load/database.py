from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Float

tmp_metadata = MetaData()

tmp_dim_time = Table('tmp_dim_time', tmp_metadata,
                 Column('id', Integer, autoincrement=True, primary_key=True),
                 Column('year', Integer),
                 Column('month', Integer),
                 Column('day', Integer),
                 Column('quarter', Integer)
                 )

tmp_dim_location = Table('tmp_dim_location', tmp_metadata,
                     Column('id', Integer, primary_key=True, autoincrement=True),
                     Column('state', String(255)),
                     Column('cz_name', String(255)),
                     Column('lat', Float),
                     Column('lon', Float)
                     )

tmp_dim_source = Table('tmp_dim_source', tmp_metadata,
                   Column('id', Integer, primary_key=True, autoincrement=True),
                   Column('source', String(255))
                   )

tmp_dim_flood_cause = Table('tmp_dim_flood_cause', tmp_metadata,
                        Column('id', Integer, primary_key=True, autoincrement=True),
                        Column('flood_cause', String(255))
                        )

tmp_dim_event_type = Table('tmp_dim_event_type', tmp_metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       Column('event_type', String(255))
                       )

tmp_dim_wfo = Table('tmp_dim_wfo', tmp_metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('wfo', String(10))
                )

tmp_dim_population_density = Table('tmp_dim_population_density', tmp_metadata,
                               Column('id', Integer, primary_key=True, autoincrement=True),
                               Column('density', Float)
                               )

tmp_fact_event = Table('tmp_fact_event', tmp_metadata,
                   Column('id', Integer, primary_key=True, autoincrement=True),
                   # Measures
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
                   # dim_time
                   Column('damage_property', Float),
                   Column('year', Integer),
                   Column('month', Integer),
                   Column('month_name', String(12)),
                   Column('day', Integer),
                   Column('quarter', Integer),
                   # dim_location
                   Column('state', String(255)),
                   Column('cz_name', String(255)),
                   Column('lat', Float),
                   Column('lon', Float),
                   # dim_source
                   Column('source', String(255)),
                   # dim_flood_cause
                   Column('flood_cause', String(255)),
                   # dim_event_type
                   Column('event_type', String(255)),
                   # dim_wfo
                   Column('wfo', String(10)),
                   # dim_population_density
                   Column('density', Float)
                   )

dim_time = Table('dim_time', tmp_metadata,
                     Column('id', Integer, autoincrement=True, primary_key=True),
                     Column('year', Integer),
                     Column('month', Integer),
                     Column('day', Integer),
                     Column('quarter', Integer)
                     )

dim_location = Table('dim_location', tmp_metadata,
                         Column('id', Integer, primary_key=True, autoincrement=True),
                         Column('state', String(255)),
                         Column('cz_name', String(255)),
                         Column('lat', Float),
                         Column('lon', Float)
                         )

dim_source = Table('dim_source', tmp_metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       Column('source', String(255))
                       )

dim_flood_cause = Table('dim_flood_cause', tmp_metadata,
                            Column('id', Integer, primary_key=True, autoincrement=True),
                            Column('flood_cause', String(255))
                            )

dim_event_type = Table('dim_event_type', tmp_metadata,
                           Column('id', Integer, primary_key=True, autoincrement=True),
                           Column('event_type', String(255))
                           )

dim_wfo = Table('dim_wfo', tmp_metadata,
                    Column('id', Integer, primary_key=True, autoincrement=True),
                    Column('wfo', String(10))
                    )

dim_population_density = Table('dim_population_density', tmp_metadata,
                                   Column('id', Integer, primary_key=True, autoincrement=True),
                                   Column('density', Float)
                                   )

fact_event = Table('fact_event', tmp_metadata,
                   Column('id', Integer, primary_key=True, autoincrement=True),
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
                   Column('damage_property', Float),
                   Column('time_id', Integer, ForeignKey('dim_time.id')),
                   Column('location_id', Integer, ForeignKey('dim_location.id')),
                   Column('source_id', Integer, ForeignKey('dim_source.id')),
                   Column('flood_cause_id', Integer, ForeignKey('dim_flood_cause.id')),
                   Column('event_type_id', Integer, ForeignKey('dim_event_type.id')),
                   Column('wfo_id', Integer, ForeignKey('dim_wfo.id')),
                   Column('population_density_id', Integer, ForeignKey('dim_population_density.id'))
                   )

def create_tmp_tables(engine):
    tmp_metadata.create_all(engine)


def drop_tmp_tables(engine):
    tmp_metadata.drop_all(engine)
