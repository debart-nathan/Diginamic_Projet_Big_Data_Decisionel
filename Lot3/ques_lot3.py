import pandas as pd
import happybase
import numpy as np
import matplotlib.pyplot as plt
import sys
import os

# --- Configuration HBase ---
HBASE_HOST = 'IP_DE_VOTRE_VM' # REMPLACER PAR L'IP DE VOTRE VM LINUX
HBASE_PORT = 9090
TABLE_NAME = 'FROMAGERIE_DATA'

# Colonnes nécessaires pour l'analyse
COLS_TO_FETCH = [
    # Famille c (Client)
    b'c:nomcli', b'c:prenomcli', b'c:codcli',
    # Famille d (Commande/Détails)
    b'd:datcde', b'd:timbrecde', b'd:villecli',
    # Famille l (Colis/Logistique)
    b'l:qte',
]

def scan_to_dataframe(table, scan_args={}):
    """Effectue un scan HBase et retourne un DataFrame."""
    rows = []
    
    # Le scan retourne (row_key, data_dict)
    for key, data in table.scan(columns=COLS_TO_FETCH, **scan_args):
        row = {}
        row['row_key'] = key.decode('utf-8')
        
        for k, v in data.items():
            # Ex: b'd:datcde' devient 'datcde'
            col_name = k.decode('utf-8').split(':')[-1]
            row[col_name] = v.decode('utf-8')
        
        rows.append(row)
        
    df = pd.DataFrame(rows)
    
    # Conversions de types pour l'analyse
    if not df.empty:
        df['qte'] = pd.to_numeric(df['qte'], errors='coerce').fillna(0)
        df['timbrecde'] = pd.to_numeric(df['timbrecde'], errors='coerce').fillna(0)
        
        # Extraction du codcde pour les agrégations
        # La Row Key est au format ANNEE_VILLE_CODECDE_CODEOBJ
        df['codcde'] = df['row_key'].apply(lambda x: x.split('_')[2] if len(x.split('_')) > 2 else None)
        df = df.dropna(subset=['codcde']) # Nettoyer les lignes sans codcde
        
    return df


# --- Q1 : La meilleure commande de Nantes de l’année 2020. ---
def q1_meilleure_commande_nantes_2020(table):
    """Trouve la meilleure commande de Nantes en 2020 basée sur les critères de quantité et de timbre."""
    
    # Ciblage du scan via la Row Key: '2020_NANTES'
    start_row = '2020_NANTES_'.encode('utf-8')
    end_row = '2020_NANTES|'.encode('utf-8') # Le pipe '|' est un caractère plus grand que '_' (lexicographiquement)

    print("\n--- Requête Q1: Scan des commandes 2020 à NANTES ---")
    df = scan_to_dataframe(table, {
        'row_start': start_row, 
        'row_stop': end_row
    })

    if df.empty:
        print("Aucune donnée trouvée pour Nantes 2020.")
        return pd.DataFrame()

    # Agrégation par commande (codcde)
    agg = df.groupby('codcde').agg({
        'qte': 'sum',             # Somme des quantités pour le classement (Critère 1)
        'timbrecde': 'first',     # Timbrecde est unique par commande (Critère 2)
        'villecli': 'first',
        'datcde': 'first'
    }).reset_index()
    
    # Tri selon les deux critères: (1) Somme Qte > (2) Timbrecde
    meilleure_cde = agg.sort_values(by=['qte', 'timbrecde'], ascending=[False, False]).head(1)
    
    print("\n[RÉSULTAT Q1: Meilleure Commande]")
    print(meilleure_cde)
    
    return meilleure_cde


# --- Q2 : Le nombre total de commandes effectuées entre 2010 et 2015, réparties par année ---
def q2_commandes_par_an(table):
    """Calcule le nombre de commandes uniques par année (2010 à 2015)."""
    
    print("\n--- Requête Q2: Scan des commandes 2010 à 2015 ---")
    # Scan sur les années 2010 à 2015
    start_row = '2010'.encode('utf-8')
    end_row = '2016'.encode('utf-8') # S'arrête juste avant 2016
    
    df = scan_to_dataframe(table, {
        'row_start': start_row, 
        'row_stop': end_row
    })
    
    if df.empty:
        print("Aucune donnée trouvée entre 2010 et 2015.")
        return pd.DataFrame()
    
    # Extraction de l'année à partir de la Row Key
    df['annee'] = df['row_key'].apply(lambda x: x.split('_')[0] if len(x.split('_')) > 0 else None)
    
    # Compter les commandes uniques par an. Le codcde a été extrait dans scan_to_dataframe
    commandes_par_an = df.groupby('annee')['codcde'].nunique().reset_index()
    commandes_par_an.columns = ['annee', 'nombre_commandes']
    
    print("\n[RÉSULTAT Q2: Commandes par Année]")
    print(commandes_par_an)
    
    return commandes_par_an


# --- Q3 : Le nom, le prénom, le nombre de commande et la somme des quantités d’objets du client qui a eu le plus de frais de timbrecde. ---
def q3_top_client_timbre(table):
    """Trouve le client avec le plus haut total de frais de timbrecde."""
    
    print("\n--- Requête Q3: Scan de toute la table pour agrégation client ---")
    # Scan complet (pas de filtre sur la clé nécessaire)
    df = scan_to_dataframe(table)

    if df.empty:
        print("Aucune donnée trouvée pour l'analyse client.")
        return pd.DataFrame()
        
    # Agrégation par client (codcli)
    # Note: On utilise 'codcde' et 'qte' agrégés au niveau du client
    agg = df.groupby(['codcli', 'nomcli', 'prenomcli']).agg(
        somme_timbre=('timbrecde', 'sum'),        # Le critère de classement
        somme_quantites=('qte', 'sum'),           # Somme des quantités d'objets (demandé)
        nombre_commandes=('codcde', 'nunique')    # Nombre de commandes uniques (demandé)
    ).reset_index()
    
    # Classement par somme_timbre (décroissant)
    top_client = agg.sort_values(by='somme_timbre', ascending=False).head(1)
    
    # Sélectionner et renommer les colonnes demandées dans l'énoncé
    result = top_client.rename(columns={
        'nomcli': 'nom',
        'prenomcli': 'prénom',
        'nombre_commandes': 'nombre_de_commandes',
        'somme_quantites': 'somme_des_quantités_objets'
    })[['nom', 'prénom', 'nombre_de_commandes', 'somme_des_quantités_objets']]
    
    print("\n[RÉSULTAT Q3: Top Client Timbre]")
    print(result)
    
    return result


# --- POINT D'ENTRÉE PRINCIPAL ET EXPORTS ---
if __name__ == "__main__":
    try:
        # 1. Connexion HBase
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=10000)
        connection.open()
        table = connection.table(TABLE_NAME)

        # 2. Exécution des requêtes et Exports

        # Q1: Meilleure commande 2020 Nantes (Export CSV)
        df_q1 = q1_meilleure_commande_nantes_2020(table)
        df_q1.to_csv('lot3_q1_meilleure_commande.csv', index=False)
        print("\n--> Export Q1 (CSV) terminé: lot3_q1_meilleure_commande.csv")


        # Q2: Commandes par année 2010-2015 (Export Barplot PDF)
        df_q2 = q2_commandes_par_an(table)
        
        plt.figure(figsize=(10, 6))
        plt.bar(df_q2['annee'].astype(str), df_q2['nombre_commandes'], color='darkgreen')
        plt.title("Nombre total de commandes par année (2010-2015)")
        plt.xlabel("Année")
        plt.ylabel("Nombre de commandes")
        plt.grid(axis='y', linestyle='--')
        plt.savefig('lot3_q2_commandes_par_an_barplot.pdf')
        print("--> Export Q2 (PDF - Barplot) terminé: lot3_q2_commandes_par_an_barplot.pdf")


        # Q3: Top client (Export Excel)
        df_q3 = q3_top_client_timbre(table)
        df_q3.to_excel('lot3_q3_top_client.xlsx', index=False)
        print("--> Export Q3 (Excel) terminé: lot3_q3_top_client.xlsx")

        connection.close()

    except Exception as e:
        print("\nERREUR FATALE:", file=sys.stderr)
        print(f"Assurez-vous que HBase Thrift Server est lancé sur {HBASE_HOST}:{HBASE_PORT}.", file=sys.stderr)
        print(e, file=sys.stderr)