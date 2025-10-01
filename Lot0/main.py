import argparse
import os
import pandas as pd

# ----------------------------------------------------------------------------------------------------------------
# Packages complémentaires
# ----------------------------------------------------------------------------------------------------------------
from analyse import analyse
from formatage_majuscule import convertir_majuscule_csv


# ----------------------------------------------------------------------------------------------------------------
# Fonction principale
# ----------------------------------------------------------------------------------------------------------------

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
   
    df_upper = convertir_majuscule_csv(df)

    print(df_upper.head())
    
    return 0


# ----------------------------------------------------------------------------------------------------------------
# Tests unitaires
# ----------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    main()