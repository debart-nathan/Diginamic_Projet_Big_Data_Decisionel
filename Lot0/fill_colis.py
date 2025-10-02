import pandas as pd

def fill_missing_colis(df: pd.DataFrame) -> pd.DataFrame:

    def fill_group(group: pd.DataFrame) -> pd.DataFrame:
        total_colis = int(group['Nbcolis'].iloc[0])
        existing_colis = group['Colis'].dropna().astype(int).tolist()
        expected_colis = set(range(1, total_colis + 1))
        missing_colis = sorted(expected_colis - set(existing_colis))

        # Fill missing values with available missing_colis
        group = group.copy()
        missing_indices = group['Colis'].isna()
        num_missing = missing_indices.sum()

        # Ensure we don't assign more values than available
        fill_values = missing_colis[:num_missing]
        group.loc[missing_indices, 'Colis'] = fill_values

        return group

    df_filled = df.groupby('codcde', group_keys=False).apply(fill_group)
    df_filled['Colis'] = df_filled['Colis'].astype(int)
    return df_filled

def main():
    data = {
        'codcde': [101, 101, 101, 202, 202, 303],
        'Colis': [1, None, 3, None, None, None],
        'Nbcolis': [3, 3, 3, 2, 2, 1]
    }
    df = pd.DataFrame(data)

    print("Original DataFrame:")
    print(df)

    updated_df = fill_missing_colis(df)

    print("\nUpdated DataFrame:")
    print(updated_df)

if __name__ == "__main__":
    main()
