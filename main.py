import os
CONNECTION_STRING = f"mssql+pyodbc://@{os.environ["DB_NAME"]}/hurtownie_projekt?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

from sqlalchemy import create_engine

engine = create_engine(CONNECTION_STRING)

