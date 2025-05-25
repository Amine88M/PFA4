import os
import json
import pandas as pd
from sqlalchemy import create_engine, text

# Chemin du fichier JSON
json_file_path = 'D:/4anne/PFA/hcp-extraction/data/hcp/indicateurs_hcp.json'

# Vérification si le fichier existe
if not os.path.exists(json_file_path):
    print(f"❌ Le fichier {json_file_path} n'existe pas.")
else:
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
        if isinstance(entries, list):  # S'assurer que chaque catégorie est une liste
            for entry in entries:
                if 'annee' in entry:
                    years.add(entry['annee'])

    # Insérer les années dans la table dimension_year
    with engine.connect() as connection:  # Connexion à la base de données
        for year in years:
            year_check_query = f"SELECT annee FROM dimension_year WHERE annee = :year"
            year_id_df = pd.read_sql(year_check_query, con=connection, params={"year": year})

            if year_id_df.empty:
                # Insérer l'année dans dimension_year si elle n'existe pas déjà
                insert_query = text("INSERT INTO dimension_year (annee) VALUES (:year)")
                connection.execute(insert_query, {"year": year})
                print(f"✅ L'année {year} insérée dans dimension_year.")

    # Préparer les données économiques sans les secteurs
    fact_data = []
    for indicator_category in data:
        if indicator_category != "pib_secteur":  # Exclure pib_secteur
            category_data = data[indicator_category]

            if isinstance(category_data, list):  # Assurer que c'est une liste d'entrées
                for entry in category_data:
                    year = entry['annee']
                    year_id_query = f"SELECT id_annee FROM dimension_year WHERE annee = :year"
                    year_id_df = pd.read_sql(year_id_query, con=engine, params={"year": year})

                    if not year_id_df.empty:
                        year_id = year_id_df.iloc[0]['id_annee']

                        # Ajouter les données économiques dans la liste fact_data
                        fact_data.append({
                            'id_annee': year_id,
                            'pib_total': entry.get('valeur', 0) if indicator_category == "PIB total" else None,
                            'taux_croissance': entry.get('valeur', 0) if indicator_category == "Taux de croissance" else None,
                            'inflation': entry.get('valeur', 0) if indicator_category == "Inflation" else None,
                            'taux_chomage': entry.get('valeur', 0) if indicator_category == "Taux de chômage" else None
                        })

    # Convertir les données en DataFrame
    df_fact = pd.DataFrame(fact_data)

    # Insérer les données dans la table fact_economic_data
    if not df_fact.empty:
        df_fact.to_sql('fact_economic_data', con=engine, if_exists='append', index=False)
        print("✅ Données insérées avec succès dans la table fact_economic_data.")
    else:
        print("❌ Aucune donnée à insérer.")
