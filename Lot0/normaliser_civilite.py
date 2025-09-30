import pandas as pd
import os
import io

# ----------------------------------------------------------------------------------------------------------------
# Normalisation de la Civilité des clients
# ----------------------------------------------------------------------------------------------------------------

def normaliser_civilite(chemin_fichier):
    """
    Normalise le champ 'genrecli' d'un fichier CSV selon les règles spécifiées.
    Le fichier nettoyé est sauvegardé au format Excel (*.xlsx) et une DataFrame normalisée est retournée.
    """
    df = pd.read_csv(chemin_fichier, dtype=str, encoding="utf-8")
    
    def normaliser(civilite):
        if pd.isnull(civilite):
            return civilite
        civilite = civilite.strip().lower()
        if civilite in ['m', 'm.']:
            return 'M.'
        elif civilite in ['mme', 'mlle', 'mlle.', 'mlles']:
            return 'Mme'
        elif civilite in ['m. et mme', 'm et mme']:
            return 'M. et Mme'
        else:
            return civilite

    if 'genrecli' in df.columns:
        df['genrecli'] = df['genrecli'].apply(normaliser)
    else:
        raise KeyError("Le champ 'genrecli' n'existe pas dans le fichier.")

    # Sauvegarder le fichier nettoyé au format Excel
    chemin_excel = os.path.splitext(chemin_fichier)[0] + '_nettoye.xlsx'
    df.to_excel(chemin_excel, index=False)

    return df


# ----------------------------------------------------------------------------------------------------------------
# Tests unitaires
# ----------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    # Création d'un DataFrame de test
    csv_test = io.StringIO("""id,genrecli
1,M
2,m
3,Mme
4,mlle
5,Mlles
6,M. et Mme
7,M et Mme
8,
9,Autre
""")
    df_test = pd.read_csv(csv_test, dtype=str)
    df_test.to_csv("test_genrecli.csv", index=False)

    df_result = normaliser_civilite("test_genrecli.csv")
    print(df_result)