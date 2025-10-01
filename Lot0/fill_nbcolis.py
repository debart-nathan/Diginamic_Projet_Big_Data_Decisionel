import pandas as pd

def fill_nbcolis(df):
    # Count colis per codcmd
    colis_count = df.groupby('codcmd')['colis'].count().reset_index()
    colis_count.rename(columns={'colis': 'colis_count'}, inplace=True)

    # Merge and fill missing nbcolis
    df = df.merge(colis_count, on='codcmd', how='left')
    df['nbcolis'] = df['nbcolis'].fillna(df['colis_count'])

    # Drop helper column
    df.drop(columns='colis_count', inplace=True)

    return df

def main():
    # Sample data with integer values
    data = {
        'codcmd': [101, 101, 101, 202, 202, 303],
        'colis': [1, 2, 3, 4, 5, 6],
        'nbcolis': [None, None, None, 2, 2, None]
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
