#Attention les valeurs gardés sont celles qui sont supérieur ou égale à la valeur renseigner 
def drop_value_colonne(df, colonne, value, verbose = True): 

    # Traitement des valeurs
    df_drop = df.loc[df[colonne] >= value]
    
    if(verbose):
        nb_before = (df[colonne] < value).sum()
        nb_after = (df_drop[colonne] < value).sum()

        # Nombre de colonnes et de lignes total avant traitement
        print(f"Nombre de ligne  dans de le DataFrame avant transformation de la colonne {colonne} : {df.shape[0]}")
        print()

        print(f"Nombre de dates inférieurs à {value} avant transformation : {nb_before}")
        print()

        print(f"Nombre de dates inférieurs à {value} après traitement : {nb_after}")
        print()

        # Nombre de colonnes et de lignes total après traitement
        print(f"Nombre de ligne dans de le DataFrame après transformation de la colonne {colonne} : {df_drop.shape[0]}")
        print()
    
    return df_drop