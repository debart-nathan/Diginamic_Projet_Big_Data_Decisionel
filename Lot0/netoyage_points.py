def netoyage_colonne_points(df): 
    # Nombre de colonnes et de lignes total avant traitement
    print(f"Nombre de ligne  dans de le DataFrame avant transformation de la colonne points : {df.shape[0]}")
    print()

    # Traitement des données Null dans la colonne points
    nb_null_before = df["points"].isnull().sum()
    df_dropna = (df.dropna(subset=["points"]))
    nb_null_after = df_dropna["points"].isnull().sum()

    print(f"Nombre de null dans la colonne points avant transformation : {nb_null_before}")
    print()

    print(f"Nombre de null dans la colonne points après traitement : {nb_null_after}")
    print()

    # Traitement des points négatifs
    nb_point_before_0 = (df_dropna["points"] < 0).sum()
    df_drop_point = df_dropna.loc[df_dropna["points"] >= 0]
    nb_point_after = (df_drop_point["points"] < 0).sum()

    print(f"Nombre de point inférieurs à 0 avant transformation : {nb_point_before_0}")
    print()

    print(f"Nombre de point inférieurs à 0 après traitement : {nb_point_after}")
    print()

    # Nombre de colonnes et de lignes total après traitement
    print(f"Nombre de ligne  dans de le DataFrame après transformation de la colonne points : {df_drop_point.shape[0]}")
    print()
    
    return df_drop_point