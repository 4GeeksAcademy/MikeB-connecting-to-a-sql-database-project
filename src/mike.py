import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

# 1) Conectar a la base de datos con SQLAlchemy
def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT") 
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    
engine = connect()

if engine is None:
    exit() 

try:
    # 2) Crear las tablas
    with open('sql/create.sql', 'r') as sql_file:
        sql_script=sql_file.read()
    try:
        with engine.connect() as connection:
            connection.execute(text(sql_script))       
    except SQLAlchemyError as e:
        print (f"An error ocurred: Table already exist or Error creating table")
        
    # 3) Insertar datos
    with open('sql/insert.sql', 'r') as sql_file:
        sql_script=sql_file.read()

    try:
        with engine.connect() as connection:
            connection.execute(text(sql_script))
    except SQLAlchemyError as e:
        print (f"An error ocurred: Data already exist or Error inserting data")

except Exception as e:
    print(f"An Error Ocurred! {e}")

# 4) Usar Pandas para leer y mostrar una tabla

# with engine.connect() as connection:
#     result = connection.execute(text("SELECT * FROM publishers"))
#     rows = result.fetchall()

#     for row in rows:
#         print(row)

result_dataFrame=pd.read_sql("Select * from publishers;", engine)
print(result_dataFrame)  