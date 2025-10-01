import pandas as pd
import os
import io

# ----------------------------------------------------------------------------------------------------------------
# Normalisation de la Civilité des clients
# ----------------------------------------------------------------------------------------------------------------

def normaliser_civilite(df):
    """
    Normalise le champ 'genrecli' d'un fichier CSV selon les règles spécifiées.
    Le fichier nettoyé est sauvegardé au format Excel (*.xlsx) et une DataFrame normalisée est retournée.
    """
    
    def normaliser(civilite):
        if pd.isnull(civilite):
            return civilite
        civilite = civilite.strip().lower()
        if civilite in ['m', 'm.']:
            return 'M.'
        elif civilite in ['mme', 'mlle', 'melle.','melle', 'melles','mlle','mlles']:
            return 'Mme'
        elif civilite in ['m. et mme', 'm et mme','m. & mme', 'm & mme']:
            return 'M. & Mme'
        else:
            return civilite

    if 'genrecli' in df.columns:
        df['genrecli'] = df['genrecli'].apply(normaliser)
    else:
        raise KeyError("Le champ 'genrecli' n'existe pas dans la dataframe.")


    return df


# ----------------------------------------------------------------------------------------------------------------
# Tests unitaires
# ----------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    # Création d'un DataFrame de test
    data = {
        'id':[1,2,3,4,5,6,7,8,9],
        'genrecli': ['M','m','Mme','mlle','melle.','Mlles','M. et Mme','M et Mme','','Autre']
    }
    
    df_test = pd.DataFrame(data)

    df_result = normaliser_civilite(df_test)
    print(df_result)
