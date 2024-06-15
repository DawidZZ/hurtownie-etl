def load_data_to_temp_dim_tables(engine, data, strategy='replace'):
    with engine.connect() as conn:
        time_data = data[['YEAR', 'MONTH', 'BEGIN_DAY']].drop_duplicates()
        time_data.columns = ['year', 'month', 'day']
        time_data['quarter'] = ((time_data['month'] - 1) // 3) + 1
        time_data.to_sql('tmp_dim_time', conn, if_exists=strategy, index=False)

        location_data = data[['STATE', 'CZ_NAME', 'BEGIN_LAT', 'BEGIN_LON']].drop_duplicates()
        location_data.columns = ['state', 'cz_name', 'lat', 'lon']
        location_data.to_sql('tmp_dim_location', conn, if_exists=strategy, index=False)

        source_data = data[['SOURCE']].drop_duplicates()
        source_data.columns = ['source']
        source_data.to_sql('tmp_dim_source', conn, if_exists=strategy, index=False)

        flood_cause_data = data[['FLOOD_CAUSE']].drop_duplicates()
        flood_cause_data.columns = ['flood_cause']
        flood_cause_data.to_sql('tmp_dim_flood_cause', conn, if_exists=strategy, index=False)

        event_type_data = data[['EVENT_TYPE']].drop_duplicates()
        event_type_data.columns = ['event_type']
        event_type_data.to_sql('tmp_dim_event_type', conn, if_exists=strategy, index=False)

        wfo_data = data[['WFO']].drop_duplicates()
        wfo_data.columns = ['wfo']
        wfo_data.to_sql('tmp_dim_wfo', conn, if_exists=strategy, index=False)

        population_density_data = data[['population_density']].drop_duplicates()
        population_density_data.columns = ['density']
        population_density_data.to_sql('tmp_dim_population_density', conn, if_exists=strategy, index=False)
        
        conn.close()