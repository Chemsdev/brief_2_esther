# SQL ALCHEMY
from sqlalchemy import create_engine

# Autres
import os
from   dotenv import load_dotenv
import pandas as pd

DATABASE = [
    "DB_HOST",
    "DB_USERNAME",
    "DB_PASSWORD",
    "DB_DATABASE"
]

# ====================================================>

# Fonction configuration ENV.
def set_confg(liste_connexion:list):
    load_dotenv()
    host     = os.environ.get(liste_connexion[0])
    user     = os.environ.get(liste_connexion[1])
    password = os.environ.get(liste_connexion[2])
    database = os.environ.get(liste_connexion[3])
    config = {
        "host"     : host,
        "user"     : user,
        "password" : password,
        "database" : database,
    }  
    return config

# ====================================================>

# Fonction connexion SQL.
def connect_mysql(config:dict):
    host     = config.get('host','')
    user     = config.get('user','')
    password = config.get('password','')
    database = config.get('database','')
    engine   = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
    return engine.connect()

# ====================================================>

# Quelques requetes.
def requetes(table:str):
    
    config_sql   = set_confg(liste_connexion=DATABASE)
    engine_mysql = connect_mysql(config=config_sql)
    
    # Requête 1 : Récupérer la première ligne
    sql1 = f"SELECT * FROM {table} LIMIT 1;"
    df1 = pd.read_sql(sql1, con=engine_mysql)
    print("Première ligne:")
    print(df1)
    
    # Requête 3 : Faire un SELECT * FROM
    sql2 = f"SELECT * FROM {table};"
    df2 = pd.read_sql(sql2, con=engine_mysql)
    print("Toutes les lignes:")
    print(df2)
    engine_mysql.close()
    return df1, df2
