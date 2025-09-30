def drop_null_colonne(df, nom_colonne, verbose = True): 

    # Traitement des données Null dans la colonne chosi
    df_dropna = (df.dropna(subset=[nom_colonne]))

    if(verbose) :
        nb_null_before = df[nom_colonne].isnull().sum()
        nb_null_after = df_dropna[nom_colonne].isnull().sum()
        # Nombre de colonnes et de lignes total avant traitement
        print(f"Nombre de ligne dans de le DataFrame avant transformation de la colonne {nom_colonne} : {df.shape[0]}")
        print()

        print(f"Nombre de null dans la colonne {nom_colonne} avant transformation : {nb_null_before}")
        print()

        print(f"Nombre de null dans la colonne {nom_colonne} après traitement : {nb_null_after}")
        print()

        # Nombre de colonnes et de lignes total après traitement
        print(f"Nombre de ligne dans de le DataFrame après transformation de la colonne {nom_colonne} : {df_dropna.shape[0]}")
        print()
        
    return df_dropna