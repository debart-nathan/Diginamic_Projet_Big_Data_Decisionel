import argparse
import os
from numpy import False_
import pandas as pd
from drop_null_by_colonne import drop_null_colonne
from fill_nbcolis import fill_nbcolis
from filtre_colonne_by_value import drop_value_colonne
from normaliser_civilite import normaliser_civilite
from supprimer_doublons import supprimer_doublons_csv
from clean_text_columns import clean_text_columns
from drop_column_prixcond import drop_column_prixcond
from analyse import analyse
from formatage_majuscule import convertir_majuscule_csv


def main() -> int:
    # Set up argument parser
    
    parser = argparse.ArgumentParser(description="Process a file path.")
    parser.add_argument("filepath", type=str, help="Path to the input file")



    # Parse arguments
    args = parser.parse_args()

    # Access the file path
    file_path = args.filepath

    df = pd.read_csv(file_path)
    analyse(df)

    ##
    ## FORMATAGE
    ##

    # enleve la colonne prixcond
    df = drop_column_prixcond(df)

    # remplis les cellules vide par None
    df = df.where(pd.notnull(df), None)

    # Unicode vers ASCII (enlever les accent)
    df = clean_text_columns(df)

    # Normalisation de la colone genrecli (civilite)
    df = normaliser_civilite(df)

    # Formatter certaines colones en majuscule
    df = convertir_majuscule_csv(df)

    ##
    ## RESTRUCTURATION
    ##

    # Supperssion des doublons
    df= supprimer_doublons_csv(df)

    # supression des nuls
    df = drop_null_colonne(df,"datcde")
    df = drop_null_colonne(df,"qte")
    df = drop_null_colonne(df,"points")

    # suppression des valeurs indésirable
    df = drop_value_colonne(df,'datcde', value = "2004-01-01 00:00:00")
    df = drop_value_colonne(df,'points', value = 0)


    ##
    ## REASSIGNEMENT
    ##


    df= fill_nbcolis(df)
    df = df.where(pd.notnull(df), 'NULL')
    df['departement']= df['cpcli'].astype(str).str.zfill(5).str[:2]

    ##
    ## SORTIE
    ##

    analyse(df)

    exit_path= "./Lot0/output/" + os.path.basename(file_path)
    df.to_csv(exit_path,index=False)
    
    return 0

if __name__ == '__main__':
    main()