import pandas as pd

def fill_missing_colis(df: pd.DataFrame) -> pd.DataFrame:

    def fill_group(group: pd.DataFrame) -> pd.DataFrame:
        total_colis = int(group['nbcolis'].iloc[0])
        existing_colis = group['colis'].dropna().astype(int).tolist()
        expected_colis = set(range(1, total_colis + 1))
        missing_colis = sorted(expected_colis - set(existing_colis))

        # Fill missing values with available missing_colis
        group = group.copy()
        missing_indices = group['colis'].isna()
        num_missing = missing_indices.sum()

        # Ensure we don't assign more values than available
        fill_values = missing_colis[:num_missing]
        group.loc[missing_indices, 'colis'] = fill_values

        return group

    df_filled = df.groupby('codcmd', group_keys=False).apply(fill_group)
    df_filled['colis'] = df_filled['colis'].astype(int)
    return df_filled

def main():
    data = {
        'codcmd': [101, 101, 101, 202, 202, 303],
        'colis': [1, None, 3, None, None, None],
        'nbcolis': [3, 3, 3, 2, 2, 1]
    }
    df = pd.DataFrame(data)

    print("Original DataFrame:")
    print(df)

    updated_df = fill_missing_colis(df)

    print("\nUpdated DataFrame:")
    print(updated_df)

if __name__ == "__main__":
    main()
