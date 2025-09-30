def netoyage_colonne_qte(df): 
    # Nombre de lignes total avant traitement
    print(f"Nombre de ligne dans de le DataFrame avant transformation de la colonne qte : {df.shape[0]}")
    print()

    # Traitement des données Null dans la colonne qte
    nb_null_before = df["qte"].isnull().sum()
    df_dropna = (df.dropna(subset=["qte"]))
    nb_null_after = df_dropna["qte"].isnull().sum()

    print(f"Nombre de null dans la colonne qte avant transformation : {nb_null_before}")
    print()

    print(f"Nombre de null dans la colonne qte après traitement : {nb_null_after}")
    print()

    # Nombre de colonnes et de lignes total après traitement
    print(f"Nombre de ligne dans de le DataFrame après transformation de la colonne qte : {df_dropna.shape[0]}")
    print()
    
    return df_dropna