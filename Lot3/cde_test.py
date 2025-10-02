import pandas as pd
import happybase
import sys
import os
import meilleure_cde as mc

# --- Configuration HBase ---
HBASE_HOST = 'node195229-hadoop-2025-d04-etudiant03.sh1.hidora.com'  # IP/hostname de ta VM HBase
HBASE_PORT = 11620
TABLE_NAME = 'FROMAGERIE_DATA'

# Colonnes nécessaires (HBase)
COLS_TO_FETCH_Q1 = [
    b'd:datcde', b'd:timbrecde', b'd:villecli',
    b'l:qte',
]


# ---------------------------
# Fonction pour lire HBase → DataFrame
# ---------------------------
def scan_to_dataframe(table, cols):
    """
    Récupère les données HBase et les transforme en DataFrame Pandas
    """
    rows = []
    for key, data in table.scan(columns=cols):
        row = {
            'codcde': key.decode('utf-8'),
            'datcde': data.get(b'd:datcde', b'').decode('utf-8'),
            'timbrecde': data.get(b'd:timbrecde', b'0').decode('utf-8'),
            'villecli': data.get(b'd:villecli', b'').decode('utf-8'),
            'qte': data.get(b'l:qte', b'0').decode('utf-8'),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    return df


# ---------------------------
# Point d'exécution principal
# ---------------------------
if __name__ == "__main__":

    # Chemin d'export local (adapter à ton projet)
    OUTPUT_DIR = r"C:\Users\Admin\Documents\Projet fil rouge\Projet Conception et développement d'une solution de collecte, stockage et traitement de données\Diginamic_Projet_Big_Data_Decisionel\Lot3"
    output_csv = os.path.join(OUTPUT_DIR, "meilleure_commande_nantes_2020_hbase.csv")

    try:
        # 1. Connexion HBase
        print(f"Tentative de connexion à HBase sur {HBASE_HOST}:{HBASE_PORT}...")
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=10000)
        connection.open()
        table = connection.table(TABLE_NAME)

        # 2. Charger les données de HBase dans un DataFrame
        df = scan_to_dataframe(table, COLS_TO_FETCH_Q1)

        if df.empty:
            print("\n--- Aucun enregistrement trouvé dans HBase ---")
            sys.exit(0)

        # 3. Exécuter la logique métier (fonction dans meilleure_cde.py)
        resultat = mc.meilleure_commande_nantes_2020(df)

        # 4. Export CSV
        if not resultat.empty:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            resultat_export = resultat[['codcde', 'datcde', 'somme_qte', 'timbrecde_cmd', 'villecli']]
            resultat_export.to_csv(output_csv, index=False, sep=";")
            print(f"\n Résultat exporté dans : {output_csv}")
        else:
            print("\n--- Aucun résultat trouvé pour NANTES en 2020 ---")

        # 5. Fermer la connexion
        connection.close()

    except happybase.errors.ConnectionError as e:
        print("\n ERREUR DE CONNEXION:", file=sys.stderr)
        print(f"Impossible de se connecter à HBase ({HBASE_HOST}:{HBASE_PORT}). "
              f"Vérifie que Thrift est lancé et que le pare-feu est ouvert.", file=sys.stderr)
        print(e, file=sys.stderr)
    except Exception as e:
        print("\n ERREUR D'EXÉCUTION:", file=sys.stderr)
        print(e, file=sys.stderr)
