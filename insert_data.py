import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

# Chemin du fichier JSON
json_file_path = 'data/hcp/indicateurs_hcp.json'

# Vérification si le fichier existe
if not os.path.exists(json_file_path):
    print(f"❌ Le fichier {json_file_path} n'existe pas.")
    exit()

print(f"✅ Le fichier {json_file_path} a été trouvé.")

# Connexion à la base de données MySQL
engine = create_engine('mysql+pymysql://root:@localhost/economic_data_warehouse')

# Fonction pour charger le fichier JSON
def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

# Charger les données du fichier JSON
data = load_json(json_file_path)

# Extraire les années uniques du JSON
years = set()
for category, entries in data.items():
    if isinstance(entries, list):
        for entry in entries:
            if 'annee' in entry:
                years.add(entry['annee'])

# Insérer les années dans la table dimension_year
with engine.connect() as connection:
    for year in years:
        year_check_query = "SELECT annee FROM dimension_year WHERE annee = %s"
        year_id_df = pd.read_sql(year_check_query, con=connection, params=(year,))

        if year_id_df.empty:
            insert_query = text("INSERT INTO dimension_year (annee) VALUES (:year)")
            connection.execute(insert_query, {"year": year})
            print(f"✅ L'année {year} insérée dans dimension_year.")

# Préparer les données économiques
fact_data = []
for indicator_category in data:
    if indicator_category != "pib_secteur":  # Exclure pib_secteur
        category_data = data[indicator_category]

        if isinstance(category_data, list):
            for entry in category_data:
                year = entry['annee']
                year_id_query = "SELECT id_annee FROM dimension_year WHERE annee = %s"
                year_id_df = pd.read_sql(year_id_query, con=engine, params=(year,))

                if not year_id_df.empty:
                    year_id = year_id_df.iloc[0]['id_annee']

                    # Ajouter les données économiques dans la liste fact_data
                    fact_data.append({
                        'id_annee': year_id,
                        'pib_total': entry.get('valeur', 0) if indicator_category == "pib_total" else None,
                        'taux_croissance': entry.get('valeur', 0) if indicator_category == "taux_croissance" else None,
                        'inflation': entry.get('valeur', 0) if indicator_category == "inflation" else None,
                        'taux_chomage': entry.get('valeur', 0) if indicator_category == "taux_chomage" else None
                    })

# Convertir les données en DataFrame
df_fact = pd.DataFrame(fact_data)

# Insérer les données dans la table fact_economic_data
if not df_fact.empty:
    df_fact.to_sql('fact_economic_data', con=engine, if_exists='append', index=False)
    print("✅ Données insérées avec succès dans la table fact_economic_data.")
else:
    print("❌ Aucune donnée à insérer.")

# Traitement du PIB par secteur
if "pib_secteur" in data:
    secteur_data = []
    for entry in data["pib_secteur"]:
        year = entry['annee']
        year_id_query = "SELECT id_annee FROM dimension_year WHERE annee = %s"
        year_id_df = pd.read_sql(year_id_query, con=engine, params=(year,))

        if not year_id_df.empty:
            year_id = year_id_df.iloc[0]['id_annee']
            
            # Récupérer le secteur et s'assurer qu'il est de type CHAR
            secteur = entry.get('indicateur', '').strip()  # Ajouter .strip() pour enlever les espaces superflus
            
            # Ajouter les données du secteur
            secteur_data.append({
                'id_annee': year_id,
                'secteur': secteur,  # Assurez-vous que le secteur est bien pris ici
                'valeur': entry.get('valeur', 0),
                'pourcentage': entry.get('pourcentage', 0)
            })

    # Convertir les données en DataFrame
    df_secteur = pd.DataFrame(secteur_data)

    # Insérer les données dans la table fact_pib_secteur
    if not df_secteur.empty:
        df_secteur.to_sql('fact_pib_secteur', con=engine, if_exists='append', index=False)
        print("✅ Données du PIB par secteur insérées avec succès.")
    else:
        print("❌ Aucune donnée de secteur à insérer.")
