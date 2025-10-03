#!/usr/bin/env python
# -*-coding:utf-8 -*

import happybase
import sys
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
from dotenv import load_dotenv
from meilleure_cde_nantes_2020 import scan_to_dataframe, meilleure_commande_nantes_2020
from Total_commande_2010_2015_par_annee import filtre_hbase_2010_2015, total_commande_par_annee_2010_2015, plot_resultat_total_commande_par_annee
from extraire_client_max_timbre import extraire_client_max_timbres

load_dotenv()

def main():

    ## Connexion a la base de données
    try:
        HBASE_HOST = os.getenv("HBASE_HOST")
        HBASE_PORT = int(os.getenv("HBASE_PORT"))
        TABLE_NAME = os.getenv("TABLE_NAME")
    except Exception as e:
        print(f"Error: {e}")
    try:
        connection = happybase.Connection(HBASE_HOST,HBASE_PORT)
        table = connection.table(TABLE_NAME)
    except Exception as e:
        print("Connetion error: {0}".format(e), file=sys.stderr)
        sys.exit(1)
    

    ## Meilleur commande de nantes en 2020
    
    # Colonnes à récupérer depuis HBase
    COLS_TO_FETCH = [
        b'commande:codcde',
        b'commande:datcde',
        b'commande:timbrecde',
        b'client:villecli',
        b'detail_commande:qte'
    ]

    # Charger les données HBase dans un DataFrame
    df = scan_to_dataframe(table, COLS_TO_FETCH)

    # Appliquer la logique métier
    resultat = meilleure_commande_nantes_2020(df)

    # Exporter le résultat en format csv
    if not resultat.empty:
        OUTPUT_DIR = "./Lot3/output/"
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        output_csv = os.path.join(OUTPUT_DIR, "meilleure_commande_nantes_2020_hbase.csv")

        resultat.to_csv(output_csv, index=False, sep=";")
        print(f"\n Résultat exporté dans : {output_csv}")
    

    ## Total des commandes entre les annees 2010 et 2015

    # Filtrer les colonnes dans hbase
    data = filtre_hbase_2010_2015(table)

    # Transformer les donnees en dataframe et realisation des calculs
    df = total_commande_par_annee_2010_2015(data)

    # Plot le resultat et l'export en pdf
    plot_resultat_total_commande_par_annee(df)


    ## Nom, prenom, nombre de commande et somme des quantites d'objets du client ayant eu le plus de frais de timbre commandes

    # Lecture de la base de donnees et filtre pour recuperer le client ayant eu le plus de frais de timbre commandes
    resultat = extraire_client_max_timbres(table)

    # Sauvgarder le resultat sous format excel
    df = pd.DataFrame(resultat, index = [0])
    df.to_excel("./Lot3/output/client_max_timbre.xlsx")
    

    #Fermer la connexion à la base de donnees
    connection.close()


if __name__ == '__main__':
    main()
