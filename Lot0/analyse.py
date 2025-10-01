def analyse(df):

    print("DESCRIBE")
    print(df.describe().to_string())

    print('NULL')

    #fait la somme des null dans les sommes
    print("Nombre de null par colone ayant des null",df[df.columns[df.isnull().any()]].isnull().sum())
    print("Nombre de N/A par colone ayant des null",df[df.columns[df.isna().any()]].isna().sum())
    #calcul la fréquence de nombre de null dans les colonnes
    freq_null = df[df.columns[df.isnull().any()]].isnull().sum() * 100 / df.shape[0]
    print('Frequence des NULL',freq_null)


    #freq_null.sort_values(ascending=False).plot(kind = "bar", x = freq_null.index)

    print(df.head().to_string()) 

