CONNECTION_STRING = "mssql+pyodbc://@DESKTOP-6NDP85E/hurtownie_projekt?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"

from sqlalchemy import create_engine

engine = create_engine(CONNECTION_STRING)

