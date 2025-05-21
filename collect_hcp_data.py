import requests
import json
from datetime import datetime
import os
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration
OUTPUT_FOLDER = "data/hcp"
OUTPUT_FILE = "indicateurs_hcp.json"

# Configuration des retries pour les requêtes HTTP
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

# Création du dossier de sortie
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def get_world_bank_data(indicator_code, indicator_name, unit):
    """
    Récupère les données de la Banque Mondiale
    """
    try:
        url = f"https://api.worldbank.org/v2/country/MA/indicator/{indicator_code}?format=json&per_page=100"
        r = session.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()[1]
        return [{
            "annee": int(d["date"]),
            "valeur": float(d["value"]),
            "unite": unit,
            "pays": "Maroc",
            "indicateur": indicator_name
        } for d in data if d["value"] is not None]
    except Exception as e:
        print(f"Erreur lors de la récupération des données {indicator_name}: {str(e)}")
        return []

def get_indicateurs():
    """
    Récupère tous les indicateurs économiques
    """
    try:
        # Définition des indicateurs à récupérer
        indicators = {
            # Indicateurs existants
            "NY.GDP.MKTP.CD": {
                "name": "PIB total",
                "unit": "USD"
            },
            "NY.GDP.MKTP.KD.ZG": {
                "name": "Taux de croissance",
                "unit": "%"
            },
            "FP.CPI.TOTL.ZG": {
                "name": "Inflation",
                "unit": "%"
            },
            "SL.UEM.TOTL.ZS": {
                "name": "Taux de chômage",
                "unit": "%"
            },
            "NV.AGR.TOTL.ZS": {
                "name": "Agriculture",
                "unit": "% du PIB"
            },
            "NV.IND.TOTL.ZS": {
                "name": "Industrie",
                "unit": "% du PIB"
            },
            "NV.SRV.TOTL.ZS": {
                "name": "Services",
                "unit": "% du PIB"
            },
            
            # Commerce extérieur
            "NE.EXP.GNFS.CD": {
                "name": "Exportations",
                "unit": "USD"
            },
            "NE.IMP.GNFS.CD": {
                "name": "Importations",
                "unit": "USD"
            },
            "NE.RSB.GNFS.CD": {
                "name": "Balance commerciale",
                "unit": "USD"
            },
            "NE.TRD.GNFS.ZS": {
                "name": "Commerce extérieur",
                "unit": "% du PIB"
            },
            
            # Secteur bancaire
            "FR.INR.DPST": {
                "name": "Taux d'intérêt sur les dépôts",
                "unit": "%"
            },
            "FR.INR.LEND": {
                "name": "Taux d'intérêt sur les prêts",
                "unit": "%"
            },
            "FB.BNK.CAPA.ZS": {
                "name": "Capital bancaire",
                "unit": "% des actifs"
            },
            "FB.CBK.BRCH.P5": {
                "name": "Succursales bancaires",
                "unit": "pour 100 000 adultes"
            },
            "FB.ATM.TOTL.P5": {
                "name": "Distributeurs automatiques",
                "unit": "pour 100 000 adultes"
            },
            "FS.AST.DOMS.GD.ZS": {
                "name": "Crédit domestique",
                "unit": "% du PIB"
            },
            
            # Dette publique
            "GC.DOD.TOTL.GD.ZS": {
                "name": "Dette publique totale",
                "unit": "% du PIB"
            },
            "GC.DOD.TOTL.CN": {
                "name": "Dette publique totale",
                "unit": "Devise locale"
            },
            "GC.DOD.TOTL.CD": {
                "name": "Dette publique totale",
                "unit": "USD"
            },
            "GC.DOD.TOTL.DT.DS": {
                "name": "Service de la dette",
                "unit": "USD"
            }
        }

        # Récupération des données
        resultats = {
            "pib_total": [],
            "taux_croissance": [],
            "inflation": [],
            "taux_chomage": [],
            "pib_secteur": [],
            "commerce_exterieur": [],
            "secteur_bancaire": [],
            "dette_publique": []
        }

        for code, info in indicators.items():
            data = get_world_bank_data(code, info["name"], info["unit"])
            
            if info["name"] == "PIB total":
                resultats["pib_total"] = data
            elif info["name"] == "Taux de croissance":
                resultats["taux_croissance"] = data
            elif info["name"] == "Inflation":
                resultats["inflation"] = data
            elif info["name"] == "Taux de chômage":
                resultats["taux_chomage"] = data
            elif info["name"] in ["Exportations", "Importations", "Balance commerciale", "Commerce extérieur"]:
                resultats["commerce_exterieur"].extend(data)
            elif info["name"] in ["Taux d'intérêt sur les dépôts", "Taux d'intérêt sur les prêts", 
                                "Capital bancaire", "Succursales bancaires", 
                                "Distributeurs automatiques", "Crédit domestique"]:
                resultats["secteur_bancaire"].extend(data)
            elif info["name"] in ["Dette publique totale", "Service de la dette"]:
                resultats["dette_publique"].extend(data)
            else:
                resultats["pib_secteur"].extend(data)

        # Ajout des métadonnées
        resultats["metadata"] = {
            "source": "Banque Mondiale",
            "date_collecte": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "description": "Données économiques du Maroc collectées depuis la Banque Mondiale"
        }

        return resultats
    except Exception as e:
        print(f"Erreur lors de la collecte des données: {str(e)}")
        return None

def save_to_json(data, output_path):
    """
    Sauvegarde les données au format JSON
    """
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"✅ Données sauvegardées avec succès dans {output_path}")
    except Exception as e:
        print(f"❌ Erreur lors de la sauvegarde des données: {str(e)}")

if __name__ == "__main__":
    print("🔄 Collecte des données économiques...")
    
    # Récupération des données
    donnees = get_indicateurs()
    
    if donnees:
        # Sauvegarde des données
        output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILE)
        save_to_json(donnees, output_path)
    else:
        print("❌ Aucune donnée n'a pu être collectée") 