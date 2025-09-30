import pandas as pd

def supprimer_doublons_csv(df, colonnes=None):

    # Supprimer les doublons
    df_sans_doublons = df.drop_duplicates(subset=colonnes)

    return df_sans_doublons


