import os
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

# Chemin du fichier JSON converti
json_file_path = 'D:/4anne/PFA/hcp-extraction/data/hcp/indicateurs_hcp_conv.json'

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

    # Transformer les données pour les insérer dans la base de données

    # Table des années
    years = set([entry['annee'] for entry in data['pib_total']])
    year_data = [{'annee': year} for year in years]

    # Connexion à la base de données et récupération des années existantes
    existing_years_query = "SELECT annee FROM dimension_year"
    existing_years_df = pd.read_sql(existing_years_query, con=engine)
    existing_years = set(existing_years_df['annee'].tolist())

    # Ne conserver que les années qui n'existent pas encore dans la base de données
    new_years = [year for year in years if year not in existing_years]

    # Préparer les données à insérer dans la table dimension_year
    year_data = [{'annee': year} for year in new_years]

    # Convertir en DataFrame pandas
    df_year = pd.DataFrame(year_data)

    # Insérer les nouvelles années dans la table dimension_year
    if not df_year.empty:
        df_year.to_sql('dimension_year', con=engine, if_exists='append', index=False)
        print(f"✅ {len(df_year)} années insérées dans dimension_year.")
    else:
        print("❌ Aucune nouvelle année à insérer.")

    # Table des secteurs
    sectors = ['Agriculture', 'Industrie', 'Services']
    sector_data = [{'nom_secteur': sector} for sector in sectors]

    # Vérifier les secteurs déjà présents dans la base de données
    existing_sectors_query = "SELECT nom_secteur FROM dimension_sector"
    existing_sectors_df = pd.read_sql(existing_sectors_query, con=engine)
    existing_sectors = set(existing_sectors_df['nom_secteur'].tolist())

    # Ne conserver que les secteurs qui n'existent pas encore dans la base de données
    new_sectors = [sector for sector in sectors if sector not in existing_sectors]

    # Préparer les données à insérer dans la table dimension_sector
    sector_data = [{'nom_secteur': sector} for sector in new_sectors]

    # Convertir en DataFrame pandas
    df_sector = pd.DataFrame(sector_data)

    # Insérer les nouveaux secteurs dans la table dimension_sector
    if not df_sector.empty:
        df_sector.to_sql('dimension_sector', con=engine, if_exists='append', index=False)
        print(f"✅ {len(df_sector)} secteurs insérés dans dimension_sector.")
    else:
        print("❌ Aucune nouvelle secteur à insérer.")

    # Insérer les données économiques dans fact_economic_data
    # Correcting the insertion logic to ensure correct mapping of values
fact_data = []
for year_entry in data['pib_total']:
    year_id = pd.read_sql(f"SELECT id_annee FROM dimension_year WHERE annee = {year_entry['annee']}", con=engine).iloc[0]['id_annee']
    
    for sector in sectors:
        sector_id = pd.read_sql(f"SELECT id_secteur FROM dimension_sector WHERE nom_secteur = '{sector}'", con=engine).iloc[0]['id_secteur']
        
        # Retrieve values from the JSON data for each sector and year
        pib_total_value = next((item['valeur'] for item in data['pib_total'] if item['annee'] == year_entry['annee']), 0)
        pib_growth_rate = next((item['valeur'] for item in data['taux_croissance'] if item['annee'] == year_entry['annee']), 0)
        inflation = next((item['valeur'] for item in data['inflation'] if item['annee'] == year_entry['annee']), 0)
        unemployment_rate = next((item['valeur'] for item in data['taux_chomage'] if item['annee'] == year_entry['annee']), 0)

        # Add the data to the list of facts
        fact_data.append({
            'id_annee': year_id,
            'id_secteur': sector_id,
            'pib_total': pib_total_value,
            'taux_croissance': pib_growth_rate,
            'inflation': inflation,
            'taux_chomage': unemployment_rate
        })

df_fact = pd.DataFrame(fact_data)

# Insert the corrected data into fact_economic_data table
df_fact.to_sql('fact_economic_data', con=engine, if_exists='append', index=False)

print("✅ Données insérées avec succès dans la base de données.")
