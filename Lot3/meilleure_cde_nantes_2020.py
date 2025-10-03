import pandas as pd
import happybase
import sys
import os

# --- CONFIG HBASE ---
HBASE_HOST = "hadoop_master"  # IP/hostname de ton serveur
HBASE_PORT = 9090
TABLE_NAME = "digicheese_data"

# Colonnes à récupérer depuis HBase
COLS_TO_FETCH = [
    b'commande:codcde',
    b'commande:datcde',
    b'commande:timbrecde',
    b'client:villecli',
    b'detail_commande:qte'
]

# --- Fonction pour récupérer les données HBase dans un DataFrame ---
def scan_to_dataframe(table, cols):
    rows = []
    for key, data in table.scan(columns=cols):
        row_dict = {}
        for col in cols:
            col_name = col.decode().split(":")[1]   # Ex: commande:datcde -> datcde
            row_dict[col_name] = data.get(col, b"").decode("utf-8")
        rows.append(row_dict)
    return pd.DataFrame(rows)

# --- Logique de la meilleure commande ---
def meilleure_commande_nantes_2020(df):
    df['datcde'] = pd.to_datetime(df['datcde'], errors='coerce')
    df['qte'] = pd.to_numeric(df['qte'], errors='coerce').fillna(0)
    df['timbrecde'] = pd.to_numeric(df['timbrecde'], errors='coerce').fillna(0)
    df = df.dropna(subset=['datcde'])

    df['annee'] = df['datcde'].dt.year
    df_nantes_2020 = df[
        (df['annee'] == 2020) &
        (df['villecli'].str.upper() == 'NANTES')
    ].copy()

    if df_nantes_2020.empty:
        print("\n--- Aucune donnée trouvée pour NANTES en 2020. ---")
        return pd.DataFrame()

    agg = df_nantes_2020.groupby('codcde').agg(
        somme_qte=('qte', 'sum'),
        timbrecde_cmd=('timbrecde', 'first'),
        villecli=('villecli', 'first'),
        datcde=('datcde', 'first')
    ).reset_index()

    meilleure_cde = agg.sort_values(
        by=['somme_qte', 'timbrecde_cmd'],
        ascending=[False, False]
    ).head(1)

    return meilleure_cde


# --- POINT D’ENTRÉE ---
if __name__ == "__main__":
    try:
        print(f"Tentative de connexion à HBase sur {HBASE_HOST}:{HBASE_PORT}...")
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=10000)
        connection.open()
        table = connection.table(TABLE_NAME)

        # Charger les données HBase dans un DataFrame
        df = scan_to_dataframe(table, COLS_TO_FETCH)

        # Appliquer la logique métier
        resultat = meilleure_commande_nantes_2020(df)

        if not resultat.empty:
            OUTPUT_DIR = r"C:\Users\Admin\Documents\Projet fil rouge\Projet Conception et développement d'une solution de collecte, stockage et traitement de données\Diginamic_Projet_Big_Data_Decisionel\Lot3"
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            output_csv = os.path.join(OUTPUT_DIR, "meilleure_commande_nantes_2020_hbase.csv")

            resultat.to_csv(output_csv, index=False, sep=";")
            print(f"\n Résultat exporté dans : {output_csv}")

        connection.close()

    except Exception as e:
        print("\n ERREUR :", e, file=sys.stderr)

