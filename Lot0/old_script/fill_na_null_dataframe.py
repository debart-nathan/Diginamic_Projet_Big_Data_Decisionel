def fill_na_dataframe(df):
   df_fill = df.where(pd.notnull(df), None)
   return df_fill
