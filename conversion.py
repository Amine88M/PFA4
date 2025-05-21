import os
import json
import pandas as pd
from datetime import datetime

# Fonction pour charger un fichier JSON
def load_json(file_path):
    print(f"🔄 Chargement du fichier : {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

# Fonction pour sauvegarder un fichier JSON
def save_json(data, output_path):
    # Vérification si le fichier existe déjà et suppression
    if os.path.exists(output_path):
        print(f"❌ Le fichier {output_path} existe déjà. Suppression du fichier existant...")
        os.remove(output_path)  # Supprimer le fichier existant

    # Ajouter la date et l'heure de conversion en haut du fichier
    data["date_conversion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Sauvegarde du fichier converti
    print(f"✅ Sauvegarde du fichier converti : {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# Fonction pour convertir les secteurs économiques en valeurs monétaires
def convert_sectors_and_debt_to_monetary_value(data):
    # Convertir les données en DataFrame pandas
    pib_data = pd.DataFrame(data["pib_total"])
    pib_secteur_data = pd.DataFrame(data["pib_secteur"])
    pib_dette_data = pd.DataFrame(data["dette_publique"])

    # Remplacer les valeurs manquantes par 0
    pib_data['valeur'] = pib_data['valeur'].fillna(0)
    pib_secteur_data['valeur'] = pib_secteur_data['valeur'].fillna(0)
    pib_dette_data['valeur'] = pib_dette_data['valeur'].fillna(0)

    # Conversion des secteurs en valeurs monétaires
    print("🔄 Conversion des secteurs économiques en valeurs monétaires...")
    pib_secteur_data['valeur_monetary'] = pib_secteur_data.apply(
        lambda row: row['valeur'] * pib_data[pib_data['annee'] == row['annee']]['valeur'].values[0] / 100,
        axis=1
    )

    # Conversion de la dette publique en valeurs monétaires
    print("🔄 Conversion de la dette publique en valeurs monétaires...")
    pib_dette_data['valeur_monetary'] = pib_dette_data.apply(
        lambda row: row['valeur'] * pib_data[pib_data['annee'] == row['annee']]['valeur'].values[0] / 100,
        axis=1
    )

    # Mettre à jour l'unité à "USD"
    pib_secteur_data['unite'] = "USD"
    pib_dette_data['unite'] = "USD"

    # Ajouter les valeurs monétaires converties dans les données d'origine
    data["pib_secteur"] = pib_secteur_data.to_dict(orient='records')
    data["dette_publique"] = pib_dette_data.to_dict(orient='records')

    print("✅ Conversion terminée pour les secteurs économiques et la dette publique.")
    return data

# Fonction pour traiter tous les fichiers JSON dans un dossier et appliquer la conversion
def convert_files_in_directory(directory_path):
    print(f"🔄 Traitement des fichiers dans : {directory_path}")
    # Lister tous les fichiers dans le dossier
    files = [f for f in os.listdir(directory_path) if f.endswith(".json")]
    
    if not files:
        print("❌ Aucun fichier JSON trouvé dans le dossier.")
        return

    for filename in files:
        file_path = os.path.join(directory_path, filename)
        print(f"🔄 Traitement du fichier : {filename}")

        # Charger les données JSON
        data = load_json(file_path)

        # Appliquer la conversion
        converted_data = convert_sectors_and_debt_to_monetary_value(data)

        # Vérifier si le fichier converti existe déjà
        if filename.endswith("_conv.json"):
            output_filename = filename  # Si le fichier existe déjà avec _conv, on garde le même nom
        else:
            output_filename = filename.replace(".json", "_conv.json")  # Ajouter _conv au fichier converti

        output_path = os.path.join(directory_path, output_filename)
        
        # Sauvegarder le fichier converti (écraser si le fichier existe déjà)
        save_json(converted_data, output_path)

    print("✅ Traitement de tous les fichiers terminé.")

# Appel de la fonction avec le chemin du dossier
directory_path = "D:/4anne/PFA/hcp-extraction/data/hcp"  # Assurez-vous que ce chemin est correct
convert_files_in_directory(directory_path)
