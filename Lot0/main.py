import argparse
import os
import pandas as pd
from analyse import analyse



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




    
    
    return 0

if __name__ == '__main__':
    main()