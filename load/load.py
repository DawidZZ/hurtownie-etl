# import os
#
# from dotenv import load_dotenv
# from sqlalchemy import create_engine, inspect, text, bindparam, String
#
# # Load environment variables
# load_dotenv()
#
# # Get database name from environment variables
# db_name = os.getenv("DB_NAME")
# engine = create_engine(f"mssql+pyodbc://@{db_name}/StormEvents?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
# # Create an inspector object
# inspector = inspect(engine)
#
#
# def truncateTables():
#     table_names = filter(lambda name: name != 'tmp_details' and name != 'tmp_fact_event' and name != 'fact_event', inspector.get_table_names())
#
#
#
#     with engine.connect() as conn:
#         conn.execute(text("TRUNCATE TABLE tmp_fact_event"))
#         conn.execute(text("TRUNCATE TABLE fact_event"))
#         for table_name in table_names:
#             print(table_name)
#             str = "TRUNCATE TABLE " + table_name
#             conn.execute(text(str))
#         conn.commit()
#
# truncateTables()