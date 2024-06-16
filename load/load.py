from sqlalchemy import text


def load_data_to_dim_tables(engine, data, strategy='append'):
    with engine.connect() as conn:
        time_data = data[['YEAR', 'QUARTER',
                          'MONTH', 'BEGIN_DAY']].drop_duplicates()
        time_data.columns = ['year', 'quarter', 'month', 'day']
        time_data.to_sql('tmp_dim_time', conn,
                         if_exists=strategy, index=False, chunksize=1000)

        location_data = data[['STATE', 'CZ_NAME',
                              'BEGIN_LAT', 'BEGIN_LON']].drop_duplicates()
        location_data.columns = ['state', 'cz_name', 'lat', 'lon']
        location_data.to_sql('tmp_dim_location', conn,
                             if_exists=strategy, index=False, chunksize=1000)

        source_data = data[['SOURCE']].drop_duplicates()
        source_data.columns = ['source']
        source_data.to_sql('tmp_dim_source', conn,
                           if_exists=strategy, index=False, chunksize=1000)

        flood_cause_data = data[['FLOOD_CAUSE']].drop_duplicates()
        flood_cause_data.columns = ['flood_cause']
        flood_cause_data.to_sql(
            'tmp_dim_flood_cause', conn, if_exists=strategy, index=False, chunksize=1000)

        event_type_data = data[['EVENT_TYPE']].drop_duplicates()
        event_type_data.columns = ['event_type']
        event_type_data.to_sql('tmp_dim_event_type', conn,
                               if_exists=strategy, index=False, chunksize=1000)

        wfo_data = data[['WFO']].drop_duplicates()
        wfo_data.columns = ['wfo']
        wfo_data.to_sql('tmp_dim_wfo', conn, if_exists=strategy, index=False)

        population_density_data = data[[
            'population_density']].drop_duplicates()
        population_density_data.columns = ['density']
        population_density_data.to_sql(
            'tmp_dim_population_density', conn, if_exists=strategy, index=False, chunksize=1000)

        conn.close()


def load_data_to_temp_fact_table(engine, data, strategy='append'):
    data = data.drop(['BEGIN_YEARMONTH', 'END_YEARMONTH',
                     'END_DAY'], axis='columns')
    data.columns = ['year', 'quarter', 'month', 'month_name', 'day', 'state', 'cz_name', 'lat', 'lon', 'source', 'flood_cause', 'event_type',
                    'wfo', 'injuries_direct',
                    'injuries_indirect', 'deaths_direct', 'deaths_indirect', 'magnitude', 'injuries_total',
                    'deaths_total',
                    'magnitude_group', 'damage_group', 'damage_property',
                    'density', 'duration']
    with engine.connect() as conn:
        data.to_sql('tmp_fact_event', conn, if_exists=strategy,
                    index=False, chunksize=1000)

        conn.close()


def load_data_to_destination_tables(engine):
    with engine.connect() as conn:
        # Wczytywanie danych do tabeli docelowej dim_time
        insert_dim_time = text("""
        INSERT INTO dim_time (year, month, day, quarter)
        SELECT tmp.year, tmp.month, tmp.day, tmp.quarter
        FROM tmp_dim_time tmp
        LEFT JOIN dim_time dt
        ON tmp.year = dt.year AND tmp.month = dt.month AND tmp.day = dt.day AND tmp.quarter = dt.quarter
        WHERE dt.id IS NULL;
        """)
        conn.execute(insert_dim_time)

        # Wczytywanie danych do tabeli docelowej dim_location
        insert_dim_location = text("""
        INSERT INTO dim_location (state, cz_name, lat, lon)
        SELECT tmp.state, tmp.cz_name, tmp.lat, tmp.lon
        FROM tmp_dim_location tmp
        LEFT JOIN dim_location dl
        ON tmp.state = dl.state AND tmp.cz_name = dl.cz_name AND tmp.lat = dl.lat AND tmp.lon = dl.lon
        WHERE dl.id IS NULL;
        """)
        conn.execute(insert_dim_location)

        # Wczytywanie danych do tabeli docelowej dim_source
        insert_dim_source = text("""
        INSERT INTO dim_source (source)
        SELECT tmp.source
        FROM tmp_dim_source tmp
        LEFT JOIN dim_source ds
        ON tmp.source = ds.source
        WHERE ds.id IS NULL;
        """)
        conn.execute(insert_dim_source)

        # Wczytywanie danych do tabeli docelowej dim_flood_cause
        insert_dim_flood_cause = text("""
        INSERT INTO dim_flood_cause (flood_cause)
        SELECT tmp.flood_cause
        FROM tmp_dim_flood_cause tmp
        LEFT JOIN dim_flood_cause dfc
        ON tmp.flood_cause = dfc.flood_cause
        WHERE dfc.id IS NULL;
        """)
        conn.execute(insert_dim_flood_cause)

        # Wczytywanie danych do tabeli docelowej dim_event_type
        insert_dim_event_type = text("""
        INSERT INTO dim_event_type (event_type)
        SELECT tmp.event_type
        FROM tmp_dim_event_type tmp
        LEFT JOIN dim_event_type det
        ON tmp.event_type = det.event_type
        WHERE det.id IS NULL;
        """)
        conn.execute(insert_dim_event_type)

        # Wczytywanie danych do tabeli docelowej dim_wfo
        insert_dim_wfo = text("""
        INSERT INTO dim_wfo (wfo)
        SELECT tmp.wfo
        FROM tmp_dim_wfo tmp
        LEFT JOIN dim_wfo dw
        ON tmp.wfo = dw.wfo
        WHERE dw.id IS NULL;
        """)
        conn.execute(insert_dim_wfo)

        # Wczytywanie danych do tabeli docelowej dim_population_density
        insert_dim_population_density = text("""
        INSERT INTO dim_population_density (density)
        SELECT tmp.density
        FROM tmp_dim_population_density tmp
        LEFT JOIN dim_population_density dpd
        ON tmp.density = dpd.density
        WHERE dpd.id IS NULL;
        """)
        conn.execute(insert_dim_population_density)

        # Wczytywanie danych do tabeli docelowej fact_event
        insert_fact_event = text("""
        INSERT INTO fact_event (
            injuries_direct, injuries_indirect, injuries_total,
            deaths_direct, deaths_indirect, deaths_total,
            magnitude, magnitude_group, damage_group,
            duration, damage_property,
            time_id, location_id, source_id,
            flood_cause_id, event_type_id, wfo_id,
            population_density_id
        )
        SELECT 
            tfe.injuries_direct, tfe.injuries_indirect, tfe.injuries_total,
            tfe.deaths_direct, tfe.deaths_indirect, tfe.deaths_total,
            tfe.magnitude, tfe.magnitude_group, tfe.damage_group,
            tfe.duration, tfe.damage_property,
            dt.id AS time_id,
            dl.id AS location_id,
            ds.id AS source_id,
            dfc.id AS flood_cause_id,
            det.id AS event_type_id,
            dw.id AS wfo_id,
            dpd.id AS population_density_id
        FROM 
            tmp_fact_event tfe
        LEFT JOIN 
            dim_time dt ON tfe.year = dt.year AND tfe.month = dt.month AND tfe.day = dt.day
        LEFT JOIN 
            dim_location dl ON tfe.state = dl.state AND tfe.cz_name = dl.cz_name AND tfe.lat = dl.lat AND tfe.lon = dl.lon
        LEFT JOIN 
            dim_source ds ON tfe.source = ds.source
        LEFT JOIN 
            dim_flood_cause dfc ON tfe.flood_cause = dfc.flood_cause
        LEFT JOIN 
            dim_event_type det ON tfe.event_type = det.event_type
        LEFT JOIN 
            dim_wfo dw ON tfe.wfo = dw.wfo
        LEFT JOIN 
            dim_population_density dpd ON tfe.density = dpd.density
        WHERE 
            (dt.id, dl.id, ds.id, dfc.id, det.id, dw.id, dpd.id) NOT IN (
                SELECT 
                    fe.time_id, fe.location_id, fe.source_id,
                    fe.flood_cause_id, fe.event_type_id, fe.wfo_id, fe.population_density_id
                FROM fact_event fe
            );
        """)
        conn.execute(insert_fact_event)

        conn.commit()
        conn.close()
