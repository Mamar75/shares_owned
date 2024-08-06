# python '18_shares_owned/find_public.py' '17_data_new/13D.csv' '17_data_new/13G.csv'
import pandas as pd
import sys

files = sys.argv[1:]

df = [pd.read_csv(f, names=['file_path', 'cik_company', 'cik_owner', 'cusip'], dtype=str) for f in files]
df = pd.concat(df)
df = df.dropna(subset=['cik_company', 'cik_owner'])
print(f'The length before removing non-public company owners is {len(df)}.')

# Filter rows where 'cik_owner' values are also found in 'cik' column
filtered_df = df[df['cik_owner'].isin(df['cik_company'])]
print(f'The length after removing non-public company owners is {len(filtered_df)}')
filtered_df.to_csv('17_Data_new/public_file_data')

