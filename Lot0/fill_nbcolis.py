import pandas as pd

def fill_nbcolis(df):
    # Count Colis per codcde
    colis_count = df.groupby('codcde')['Colis'].count().reset_index()
    colis_count.rename(columns={'Colis': 'colis_count'}, inplace=True)

    # Merge and fill missing Nbcolis
    df = df.merge(colis_count, on='codcde', how='left')
    df['Nbcolis'] = df['Nbcolis'].fillna(df['colis_count'])

    # Drop helper column
    df.drop(columns='colis_count', inplace=True)

    return df

def main():
    # Sample data with integer values
    data = {
        'codcde': [101, 101, 101, 202, 202, 303],
        'Colis': [1, 2, 3, 4, 5, 6],
        'Nbcolis': [None, None, None, 2, 2, None]
    }
    df = pd.DataFrame(data)

    print("Original DataFrame:")
    print(df)

    # Apply function
    updated_df = fill_nbcolis(df)

    print("\nUpdated DataFrame:")
    print(updated_df)

if __name__ == "__main__":
    main()
