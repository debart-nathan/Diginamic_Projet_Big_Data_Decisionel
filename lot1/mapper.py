#!/usr/bin/env python3
import csv
from datetime import datetime

# Chemin vers le fichier CSV local
csv_file_path = r"C:\Users\Admin\Documents\Projet fil rouge\Projet Conception et développement d'une solution de collecte, stockage et traitement de données\Diginamic_Projet_Big_Data_Decisionel\data\dataw_fro03.csv"

# Départements à filtrer
VALID_DEPTS = {"22", "49", "53"}

# Fonctions utilitaires
def parse_date(date_str):
    """Parse la date en format dd/mm/YYYY ou YYYY-mm-dd"""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None

def parse_float(value):
    """Convertit une valeur en float, remplace la virgule par point"""
    try:
        return float(value.replace(",", "."))
    except (ValueError, AttributeError):
        return 0.0

def extract_dept(cpcli):
    """Extrait le département à partir du code postal"""
    cpcli = cpcli.strip().zfill(5)
    if not cpcli:
        return None
    # Gestion particulière pour la Corse (codes 2A et 2B)
    return cpcli[:2] if not cpcli.startswith("20") else cpcli[:3]

# Lecture du CSV
with open(csv_file_path, newline="", encoding="utf-8") as csvfile:
    reader = csv.reader(csvfile, delimiter="\t")
    for parts in reader:
        if not parts or parts[0].lower() == "datcde":
            continue
        if len(parts) < 7:
            continue

        datcde, cpcli, timbrecde, qte, libobj, codcde, villecli = parts

        # Filtrer par date
        date_obj = parse_date(datcde)
        if not date_obj or not (2011 <= date_obj.year <= 2016):
            continue

        # Filtrer par département
        dept = extract_dept(cpcli)
        if dept not in VALID_DEPTS:
            continue

        # Filtrer par timbrecli : vide ou 0
        if timbrecde.strip() and parse_float(timbrecde) != 0:
            continue

        # Quantité
        q = parse_float(qte)

        # Sortie
        print(f"{codcde}\t{villecli.upper()}\t{q}")


