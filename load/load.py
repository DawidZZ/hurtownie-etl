def load_data_to_dim_tables(engine, data, strategy='append'):
    with engine.connect() as conn:
        time_data = data[['YEAR', 'QUARTER', 'MONTH', 'BEGIN_DAY']].drop_duplicates()
        time_data.columns = ['year', 'quarter', 'month', 'day']
        time_data.to_sql('tmp_dim_time', conn, if_exists=strategy, index=False, chunksize=1000)

        location_data = data[['STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON']].drop_duplicates()
        location_data.columns = ['state', 'cz_name', 'lat', 'lon']
        location_data.to_sql('tmp_dim_location', conn, if_exists=strategy, index=False, chunksize=1000)

        source_data = data[['SOURCE']].drop_duplicates()
        source_data.columns = ['source']
        source_data.to_sql('tmp_dim_source', conn, if_exists=strategy, index=False, chunksize=1000)

        flood_cause_data = data[['FLOOD_CAUSE']].drop_duplicates()
        flood_cause_data.columns = ['flood_cause']
        flood_cause_data.to_sql('tmp_dim_flood_cause', conn, if_exists=strategy, index=False, chunksize=1000)

        event_type_data = data[['EVENT_TYPE']].drop_duplicates()
        event_type_data.columns = ['event_type']
        event_type_data.to_sql('tmp_dim_event_type', conn, if_exists=strategy, index=False, chunksize=1000)

        wfo_data = data[['WFO']].drop_duplicates()
        wfo_data.columns = ['wfo']
        wfo_data.to_sql('dim_wfo', conn, if_exists=strategy, index=False)

        population_density_data = data[['population_density']].drop_duplicates()
        population_density_data.columns = ['density']
        population_density_data.to_sql('tmp_dim_population_density', conn, if_exists=strategy, index=False, chunksize=1000)

        conn.close()


def load_data_to_temp_fact_table(engine, data, strategy='append'):
    print(data.columns)
    data = data.drop(['BEGIN_YEARMONTH','END_YEARMONTH', 'END_DAY'],axis='columns')
    data.columns = ['year', 'quarter', 'month', 'month_name', 'day', 'state', 'cz_name', 'lat', 'lon', 'source', 'flood_cause', 'event_type',
                    'wfo', 'injuries_direct',
                    'injuries_indirect', 'deaths_direct', 'deaths_indirect', 'magnitude', 'injuries_total',
                    'deaths_total',
                    'magnitude_group', 'damage_group', 'damage_property',
                    'density','duration']
    with engine.connect() as conn:
        print(data.shape)
        data.to_sql('tmp_fact_event', conn, if_exists=strategy, index=False, chunksize=1000)

        conn.close()
