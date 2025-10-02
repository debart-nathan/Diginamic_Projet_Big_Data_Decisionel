import pandas as pd

def meilleure_commande_nantes_2020(df):
    """
    Calcule la meilleure commande de NANTES en 2020 à partir d'un DataFrame.
    """
    # Convertir 'datcde' en format date
    df['datcde'] = pd.to_datetime(df['datcde'], errors='coerce')
    df['qte'] = pd.to_numeric(df['qte'], errors='coerce').fillna(0)
    df['timbrecde'] = pd.to_numeric(df['timbrecde'], errors='coerce').fillna(0)
    df = df.dropna(subset=['datcde'])

    # Extraire l'année
    df['annee'] = df['datcde'].dt.year

    # Filtrer sur Nantes en 2020
    df_nantes_2020 = df[
        (df['annee'] == 2020) &
        (df['villecli'].str.upper() == 'NANTES')
    ].copy()

    if df_nantes_2020.empty:
        print("\n--- Aucune donnée trouvée pour NANTES en 2020. ---")
        return pd.DataFrame()

    # Agrégation par commande
    agg = df_nantes_2020.groupby('codcde').agg(
        somme_qte=('qte', 'sum'),
        timbrecde_cmd=('timbrecde', 'first'),
        villecli=('villecli', 'first'),
        datcde=('datcde', 'first')
    ).reset_index()

    # Tri : QTE desc puis TIMBRE desc
    meilleure_cde = agg.sort_values(
        by=['somme_qte', 'timbrecde_cmd'],
        ascending=[False, False]
    ).head(1)

    return meilleure_cde


def meilleure_commande_nantes_2020_from_csv(file_path):
    """
    Charge un CSV et appelle la fonction générique.
    """
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Erreur : Le fichier '{file_path}' est introuvable.")
        return pd.DataFrame()

    return meilleure_commande_nantes_2020(df)


# --- Exemple d'utilisation ---
if __name__ == "__main__":
    file_path_csv = r"C:\Users\Admin\Documents\Projet fil rouge\Projet Conception et développement d'une solution de collecte, stockage et traitement de données\Diginamic_Projet_Big_Data_Decisionel\data\dataw_fro03.csv"
    
    resultat = meilleure_commande_nantes_2020_from_csv(file_path_csv)

    if not resultat.empty:
        output_csv = r"C:\Users\DAdmin\Documents\Projet fil rouge\Projet Conception et développement d'une solution de collecte, stockage et traitement de données\Diginamic_Projet_Big_Data_Decisionel\Lot3\meilleure_commande_nantes_2020.csv"
