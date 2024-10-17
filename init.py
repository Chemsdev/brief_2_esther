from tools import *
import pandas as pd



# =====================================================================================>

def create_table_ventes(terminaison:str):
    
    # Connexion à la base de données Azure.
    config_sql = set_confg(liste_connexion=DATABASE)
    engine_mysql = connect_mysql(config=config_sql)

    # Création de la table avec des clés étrangères.
    with engine_mysql.connect() as connection:
        connection.execute(f"""
            CREATE TABLE IF NOT EXISTS ventes_{terminaison} (
                date             VARCHAR(50)     NULL,
                session_id       VARCHAR(10)     NULL,
                client_id        VARCHAR(10)     NULL,
                product_id       VARCHAR(10)     NULL,
                quantity_sold    FLOAT           NULL,
                
                -- Création des clés étrangères
                FOREIGN KEY (client_id)  REFERENCES clients_{terminaison}(client_id),
                FOREIGN KEY (product_id) REFERENCES produits_{terminaison}(product_id)
            ) 
        """)
    engine_mysql.close()
    print('Table ventes créée avec succès !')
    
# =====================================================================================>

def create_table_clients(terminaison:str):
    # Connexion à la base de données Azure.
    config_sql = set_confg(liste_connexion=DATABASE)
    engine_mysql = connect_mysql(config=config_sql)

    # Création de la table clients.
    with engine_mysql.connect() as connection:
        connection.execute(f"""
            CREATE TABLE IF NOT EXISTS clients_{terminaison} (
                client_id VARCHAR(10) PRIMARY KEY,  -- Client ID est une chaîne
                sex       VARCHAR(50) NULL,
                birth     INT         NULL
            )
        """)
    engine_mysql.close()
    print('Table clients créée avec succès !')

# =====================================================================================>

def create_table_produits(terminaison:str):
    # Connexion à la base de données Azure.
    config_sql = set_confg(liste_connexion=DATABASE)
    engine_mysql = connect_mysql(config=config_sql)

    # Création de la table produits.
    with engine_mysql.connect() as connection:
        connection.execute(f"""
            CREATE TABLE IF NOT EXISTS produits_{terminaison} (
                product_id      VARCHAR(10) PRIMARY KEY,  -- Product ID est une chaîne
                category        VARCHAR(50)  NULL,
                sub_category    VARCHAR(50)  NULL,
                price           VARCHAR(50)  NULL,
                stock_quantity  INT          NULL
            )
        """)
    engine_mysql.close()
    print('Table produits créée avec succès !')

# =====================================================================================>

def insert_data_csv(name_file:str, name_table:str):
    
    # Connexion à la base de données Azure.
    config_sql = set_confg(liste_connexion=DATABASE)
    engine_mysql = connect_mysql(config=config_sql)
    
    # Ouverture des données et traitement.
    if name_table ==  "clients" or name_table == "produits":
        df = pd.read_csv(f'data_csv/{name_file}', delimiter=";")
        df = df.drop_duplicates(subset=None, keep='first', inplace=False)
        
    if name_table ==  "ventes":
        df = pd.read_csv(f'data_csv/{name_file}', delimiter=";")
    
    # Envoi des données.
    df.to_sql(f'{name_table}_csv', engine_mysql, if_exists='append', index=False)
    
    # fermeture de la connexion.
    engine_mysql.close()
    
# =====================================================================================>

def insert_data_json(name_file: str, name_table: str):
    
    # Connexion à la base de données Azure.
    config_sql = set_confg(liste_connexion=DATABASE)
    engine_mysql = connect_mysql(config=config_sql)

    # Ouverture des données et traitement.
    file_path = f'data_json/{name_file}.json'
    df = pd.read_json(file_path)

    # Traitement spécifique selon la table
    if name_table in ["clients", "produits"]:
        # Suppression des doublons
        df = df.drop_duplicates()

    # Envoi des données.
    df.to_sql(f'{name_table}_json', engine_mysql, if_exists='append', index=False)

    # Fermeture de la connexion.
    engine_mysql.close()
    
# =====================================================================================>
