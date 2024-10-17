from init import *


# ============================>
def load_csv():
    try:
        # Création des tables.
        create_table_clients(terminaison="csv")
        create_table_produits(terminaison="csv")
        create_table_ventes(terminaison="csv")

        # Insertion des données CSV.
        insert_data_csv(name_file="clients.csv",                 name_table="clients")
        insert_data_csv(name_file="produits_sous-categorie.csv", name_table="produits")
        insert_data_csv(name_file="ventes.csv",                  name_table="ventes")
        return True
    except:
        return False    
# ============================>


# ============================>
def load_json():
    try:
        # Création des tables.
        create_table_clients(terminaison="json")
        create_table_produits(terminaison="json")
        create_table_ventes(terminaison="json")
        
        # Insertion des données json
        insert_data_json(name_file="clients",                 name_table="clients")
        insert_data_json(name_file="produits_sous-categorie", name_table="produits")
        insert_data_json(name_file="ventes",                  name_table="ventes")
        return True
    except:
        return False
# ============================>


# 1) Chargement des fichiers csv.
load_csv()

# 2) Chargement des fichiers json.
load_json()

# 3) quelques requêtes.
requetes(table="ventes_csv")