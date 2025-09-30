def drop_column(df) :
    # Nombre de colonnes et de lignes total avant traitement
    print(f"Nombre de colonnes dans de le DataFrame avant drope de la colonne prixcond : {df.shape[1]}")
    print()
    
    df_drop = df.drop(columns= ["prixcond"])

    # Nombre de colonnes et de lignes total après traitement
    print(f"Nombre de colonnes dans de le DataFrame après drope de la colonne prixcond : {df_drop.shape[1]}")
    print()

    return df_drop