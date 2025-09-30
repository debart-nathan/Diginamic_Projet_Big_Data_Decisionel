import pandas as pd
import unicodedata



def clean_text_columns(df):
    def unicode_to_ascii(text):
        if isinstance(text, str):
            return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('ascii')
        return text
    text_cols = df.select_dtypes(include=['object', 'string']).columns
    df.loc[:, text_cols] = df.loc[:, text_cols].applymap(unicode_to_ascii)
    return df



def main():
    # Sample DataFrame with accented characters
    data = {
        'Name': ['José', 'Chloë', 'Mårten', 'Renée'],
        'City': ['São Paulo', 'Zürich', 'Málaga', 'Bogotá'],
        'Age': [28, 34, 45, 23]
    }

    df = pd.DataFrame(data)
    print("Original DataFrame:")
    print(df)

    cleaned_df = clean_text_columns(df)
    print("\nCleaned DataFrame:")
    print(cleaned_df)

if __name__ == "__main__":
    main()
