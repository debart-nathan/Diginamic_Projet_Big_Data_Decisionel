#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import csv
from io import StringIO

# Critères de filtrage pour le LOT 2
DEP_CIBLES = ['22', '49', '53']
ANNEE_MIN = 2011
ANNEE_MAX = 2016

# Ordre des colonnes dans le fichier CSV (basé sur ton projet)
COLUMNS = ["codcli","genrecli","nomcli","prenomcli","cpcli","villecli","codcde","datcde","timbrecli","timbrecde","Nbcolis","cheqcli","barchive","bstock","codobj","qte","Colis","libobj","Tailleobj","Poidsobj","points","indispobj","libcondit","prixcond","puobj"]

def mapper():
    # Saute l'entête du CSV (première ligne)
    try:
        next(sys.stdin)
    except StopIteration:
        return
    
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
            
        try:
            # Utilisation de StringIO pour parser la ligne CSV
            reader = csv.reader(StringIO(line), delimiter=',')
            row = next(reader)
            
            # Créer un dictionnaire pour accéder facilement aux colonnes
            data = dict(zip(COLUMNS, row))
            
            # --- 1. FILTRAGE PAR DATE ---
            annee = int(data['datcde'].split('-')[0])
            if not (ANNEE_MIN <= annee <= ANNEE_MAX):
                continue
            
            # --- 2. FILTRAGE PAR DÉPARTEMENT ---
            departement = data['cpcli'][:2]
            if departement not in DEP_CIBLES:
                continue

            # --- 3. FILTRAGE PAR TIMBRE CLIENT ---
            # Le timbrecli est non renseigné (vide/NULL) ou à 0
            timbrecli = data['timbrecli'].strip().replace('"', '').upper()
            try:
                # Si c'est un nombre, vérifier s'il est égal à 0
                if abs(float(timbrecli)) > 1e-9:
                    continue
            except ValueError:
                # Si ce n'est pas un nombre, c'est potentiellement vide/NULL
                if timbrecli not in ['', 'NULL', 'VIDE']:
                    # Si c'est un texte non-vide/non-NULL (non-conforme aux critères)
                    continue 

            # --- 4. ÉMISSION DES DONNÉES FILTRÉES ---
            # Le format doit être celui attendu par ta fonction lire_donnees()
            # ('codcde', 'cpcli', 'villecli', 'libobj', 'qte', 'timbrecde')
            
            codcde = data['codcde']
            cpcli = data['cpcli']
            villecli = data['villecli']
            libobj = data['libobj'].replace(' ', '_') # Remplacer espaces pour l'émission simple
            qte = data['qte']
            timbrecde = data['timbrecde']
            
            # Format d'émission simple (séparé par des espaces)
            # CLÉ (non utilisée ici, mais nécessaire pour MapReduce) \t VALEUR
            # Ici on utilise un espace pour séparer les champs, comme ton reducer l'implique
            output_value = "%s\t%s\t%s\t%s\t%s\t%s" % (codcde,cpcli,villecli,libobj,qte,timbrecde)

            print(output_value)
            
        except Exception as e:
            # Afficher les erreurs sur stderr et ignorer la ligne
            print("Erreur de traitement/format sur la ligne: %s - %s" % (line.strip(),e), file=sys.stderr)
            continue

if __name__ == "__main__":
    mapper()