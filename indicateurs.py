import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import mimetypes
from urllib.parse import urljoin

# Crée un dossier local pour stocker les fichiers téléchargés
os.makedirs("data", exist_ok=True)

def is_valid_excel(file_path):
    """Vérifie si le fichier est un Excel valide (xlsx ou xls)"""
    mime_type, _ = mimetypes.guess_type(file_path)
    return mime_type in [
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.ms-excel'
    ]

def extract_hcp_excels(base_url="https://www.hcp.ma"):
    print("🔍 Recherche de fichiers Excel sur le site HCP...")
    try:
        response = requests.get(base_url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        links = soup.find_all("a", href=True)
        excel_links = [urljoin(base_url, link['href']) for link in links if link['href'].endswith(('.xls', '.xlsx'))]

        for link in excel_links:
            filename = link.split("/")[-1]
            filepath = os.path.join("data", filename)

            print(f"📥 Téléchargement : {filename}")
            try:
                with requests.get(link, stream=True, timeout=15) as r:
                    with open(filepath, "wb") as f:
                        f.write(r.content)

                if is_valid_excel(filepath):
                    print(f"✅ Fichier valide : {filename}")
                else:
                    print(f"⛔ Fichier invalide supprimé : {filename}")
                    os.remove(filepath)

            except Exception as e:
                print(f"❌ Erreur de téléchargement pour {link} : {e}")

    except Exception as e:
        print(f"❌ Échec de connexion à {base_url} : {e}")

def load_all_excel_files():
    print("\n📂 Chargement des fichiers Excel valides dans 'data/' :")
    for file in os.listdir("data"):
        if file.endswith((".xls", ".xlsx")):
            file_path = os.path.join("data", file)
            try:
                df = pd.read_excel(file_path, engine='openpyxl')
                print(f"🗂️ Aperçu de {file} :")
                print(df.head(3))
            except Exception as e:
                print(f"⚠️ Erreur de lecture du fichier {file} : {e}")

if __name__ == "__main__":
    extract_hcp_excels()       # Étape 1 : Télécharger les fichiers Excel valides
    load_all_excel_files()     # Étape 2 : Charger et afficher les fichiers valides
