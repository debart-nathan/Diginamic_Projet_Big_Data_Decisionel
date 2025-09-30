import pandas as pd

def convertir_majuscule_csv(df):

    # Colonnes à mettre en majuscule si elles existent
    colonnes_a_modifier = ['nomcli', 'villecli']
    for col in colonnes_a_modifier:
        if col in df.columns:
            df[col] = df[col].str.upper()


    print(" Conversion majuscule terminée.")
    return df