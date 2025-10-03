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

load_dotenv()

def filtre_hbase_2010_2015(table):
    """
    Cette fonction interoge la base de donnees hbase. 
    Elle applique un filtre pour ne garder que les lignes entre les annnes 2010 et 2015.
    Les donnnes code commande (codcde) et date commande (datcde) sont recupere sous forme de tuple.
    Pour finir une liste de tuple est renvoye.
    
    Args:
        table (objet table): c'est la table de donnees dans hbase que l on souhaite interoger

    Returns:
        data: une liste de tuple contenant des donnees datetime et des code commandes
    """
    data = []
    count = 0
    for key, row in table.scan(filter=("SingleColumnValueFilter ('commande','datcde',>,'binary:2009-12-31 00:00:00') AND SingleColumnValueFilter ('commande','datcde',<,'binary:2016-01-01 00:00:00')")):
        try :
            codcde = float(row[b'commande:codcde'])
            date = str(row[b'commande:datcde'].decode())
            data.append((date, codcde))
            count += 1
            if count % 100 == 0:
                print("{0} lignes filtrees ".format(count))

        except Exception as e:
            print('Parsing error: {0}'.format(e))

    # Optional: print final count if not a multiple of 100
    if count % 100 != 0:
        print("{0} lignes filtrees dans toute la base de donnees".format(count))
    
    return data

def total_commande_par_annee_2010_2015(data, verbose = False):
    """
    Transforme une liste de tuple en DataFrame pandas.
    Puis transforme les donnees datetime en date puis en annee.
    Enfin les donnees sont groupees par annees et le nombre de commande sont comptees par annnes.
    Une series des resultats est renvoye.

    Args:
        data (list): une liste de tuple contenant des donnees datetime et des code commandes
        verbose (bool, optional): Affiche des prints dans le terminal pour avoir des informations suplementaires. Defaults to False.

    Returns:
        df_grp: series contenant le total des commandes par annnes
    """
    
    df = pd.DataFrame(data, columns=["datcde","codcde"])

    df['datetime'] = pd.to_datetime(df['datcde']).dt.date
    df['year'] = pd.to_datetime(df['datetime']).dt.year

    df_grp = df.groupby("year")["codcde"].count()

    if(verbose):
        print("L annee minimum dans le tableau est {0}".format(df["year"].min()))
        print("L annee maximum dans le tableau est {0}".format(df["year"].max()))
        print("Resultat du nombre total de commande par annee :")
        print(df_grp)

    return df_grp

def plot_resultat_total_commande_par_annee(df):
    """Produit un pdf contenant un diagramme en bar represantant le nombre total de commandes entre les annnes 2010 et 2015

    Args:
        df (series): series contenant le total des commandes par annnes
    """
    plt.figure(figsize=(8, 8))

    df.plot(kind="bar")

    plt.title('Nombre total de commandes effectuées entre 2010 et 2015')
    plt.ylabel("Nombre total de commandes")
    plt.xlabel("Année")

    plt.savefig("./Lot3/output/Nombre_total_de_commandes_2010_2015.pdf", format='pdf')


def main():
    """
    Se connecte à la table, realise les filtres et les caclul avant de resortir un graphique sous format pdf
    """
    try:
        hbase_host = os.getenv("HBASE_HOST")
        hbase_port = int(os.getenv("HBASE_PORT"))
        table_name = os.getenv("TABLE_NAME")
    except Exception as e:
        print(f"Error: {e}")

    try:
        connection = happybase.Connection(hbase_host,hbase_port)
        table = connection.table(table_name)
    except Exception as e:
        print("Connetion error: {0}".format(e), file=sys.stderr)
        sys.exit(1)
    
    data = filtre_hbase_2010_2015(table)
    df = total_commande_par_annee_2010_2015(data)
    plot_resultat_total_commande_par_annee(df)

    connection.close()

if __name__ == "__main__":
    main()
