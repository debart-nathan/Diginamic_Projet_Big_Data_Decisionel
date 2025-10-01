#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import csv
from io import StringIO

# Départements et période ciblés pour le LOT 2
DEP_CIBLES = ['22', '49', '53']
ANNEE_MIN = 2011
ANNEE_MAX = 2016

# Nom des colonnes dans le fichier CSV (ajuster si l'ordre change)
COLUMNS = ["codcli","genrecli","nomcli","prenomcli","cpcli","villecli","codcde","datcde","timbrecli","timbrecde","Nbcolis","cheqcli","barchive","bstock","codobj","qte","Colis","libobj","Tailleobj","Poidsobj","points","indispobj","libcondit","prixcond","puobj"]

def mapper():
    # Ignore l'entête du CSV (première ligne)
    next(sys.stdin) 
    
    for line in sys.stdin:
        # Utilisation de StringIO pour simuler un fichier pour le parser CSV
        line = line.strip()
        if not line:
            continue
            
        try:
            # Le parser CSV gère correctement les champs entre guillemets
            reader = csv.reader(StringIO(line), delimiter=',')
            row = next(reader)
            
            # Assigner les valeurs aux noms de colonnes pour plus de clarté
            data = dict(zip(COLUMNS, row))
            
            # --- 1. FILTRAGE ---
            
            # A. Filtrage par Année (datcde)
            annee = int(data['datcde'].split('-')[0])
            if not (ANNEE_MIN <= annee <= ANNEE_MAX):
                continue
            
            # B. Filtrage par Département (cpcli)
            departement = data['cpcli'][:2]
            if departement not in DEP_CIBLES:
                continue

            # C. Filtrage par Timbre Client (timbrecli) non renseigné ou à 0
            timbrecli = data['timbrecli'].strip().replace('"', '').upper()
            if timbrecli != '' and timbrecli != 'NULL' and float(timbrecli) != 0:
                 continue
            
            # --- 2. ÉMISSION ---
            
            codcde = data['codcde']
            villecli = data['villecli']
            
            # S'assurer que les valeurs numériques sont des nombres
            try:
                qte = float(data['qte'])
                timbrecde = float(data['timbrecde'])
            except ValueError:
                # Ignorer la ligne si les valeurs numériques sont invalides
                continue

            # Format de sortie : CLÉ (codcde) \t VALEUR (ville|qte|timbrecde)
            # On envoie toutes les lignes d'articles d'une commande au Reducer
            output_value = f"{villecli}|{qte}|{timbrecde}"
            print(f"{codcde}\t{output_value}")
            
        except Exception as e:
            # Gérer les lignes corrompues ou mal formatées
            # print(f"Erreur sur la ligne: {line} - {e}", file=sys.stderr)
            continue

if __name__ == "__main__":
    mapper()
