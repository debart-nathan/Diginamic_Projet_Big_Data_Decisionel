def netoyage_colonne_datcde(df): 
    # Nombre de colonnes et de lignes total avant traitement
    print(f"Nombre de ligne  dans de le DataFrame avant transformation de la colonne datcde : {df.shape[0]}")
    print()

    # Traitement des données Null dans la colonne datcde
    nb_null_before = df["datcde"].isnull().sum()
    df_dropna = (df.dropna(subset=["datcde"]))
    nb_null_after = df_dropna["datcde"].isnull().sum()

    print(f"Nombre de null dans la colonne datcde avant transformation : {nb_null_before}")
    print()

    print(f"Nombre de null dans la colonne datcde après traitement : {nb_null_after}")
    print()

    # Traitement des dates abérantes (avant 2004)
    nb_date_before_2004 = (df_dropna["datcde"] < "2004-01-01 00:00:00").sum()
    df_dropdate = df_dropna.loc[df_dropna["datcde"] >= "2004-01-01 00:00:00"]
    nb_date_after = (df_dropdate["datcde"] < "2004-01-01 00:00:00").sum()

    print(f"Nombre de dates inférieurs à 2004 avant transformation : {nb_date_before_2004}")
    print()

    print(f"Nombre de dates inférieurs à 2004 après traitement : {nb_date_after}")
    print()

    # Nombre de colonnes et de lignes total après traitement
    print(f"Nombre de ligne  dans de le DataFrame après transformation de la colonne datcde : {df_dropdate.shape[0]}")
    print()
    
    return df_dropdate