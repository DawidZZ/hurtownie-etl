import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text

# Load environment variables
load_dotenv()

# Get database name from environment variables
db_name = os.getenv("DB_NAME")
engine = create_engine(f"mssql+pyodbc://@{db_name}/StormEvents?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
# Create an inspector object
inspector = inspect(engine)


def delete_from_dim_tables():
    table_names = filter(lambda name: name != 'tmp_details' and name != 'tmp_fact_event' and name != 'fact_event', inspector.get_table_names())

    with engine.connect() as conn:
        for table_name in table_names:
            print(table_name)
            str = "DELETE FROM " + table_name
            conn.execute(text(str))
        conn.commit()