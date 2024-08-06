#  python '18_shares_owned/create_shares_owned/create_cusip_mapping.py' '17_data_new/13D.csv' '17_data_new/13G.csv'

# The objective is to establish a bijective mapping between 'cik' and 'cusip6'. In this context,
# 'cik' (Central Index Key) uniquely identifies an entity registered with the U.S. Securities and
# Exchange Commission (SEC), while 'cusip6' serves as a unique identifier for issuers of securities.
# A bijective map implies a one-to-one relationship, ensuring each 'cik' is paired with exactly one
# 'cusip6', and vice versa. This accurate mapping is crucial for linking registered entities to their
# respective issuers effectively. The method involves grouping the data by 'cusip6' and 'cik', then
# counting the frequency of each 'cik' within 'cusip6' groups. The most frequently occurring 'cik'
# for a given 'cusip6' is assumed to represent the most accurate issuer-entity relationship. This
# 'cik' is then selected for each 'cusip6', constructing the desired bijective map.

import sys
import pandas as pd

files = sys.argv[1:]

df = [pd.read_csv(f, names=['f', 'cik', 'cik_owner', 'cusip'], dtype=str) for f in files]
df = pd.concat(df)

# The next line is important to remove cases of financial institution that for a given asset
# file 13D/G as owner and subject company.
# Renaissance technology LLC as example: 17_data_new/13G\2009_02\1037389_2009-02-12_000022.txt
df = df[df['cik'] != df['cik_owner']]

df = df[['cik', 'cusip']].dropna()

df['leng'] = df.cusip.map(len)

df = df[(df.leng == 6) | (df.leng == 8) | (df.leng == 9)]

df['cusip6'] = df.cusip.str[:6]

df = df[df.cusip6 != '000000']
df = df[df.cusip6 != '0001pt']

# df['cusip8'] = df.cusip.str[:8]
# df = df[df['cusip8'].str.len() == 8]

df.cik = pd.to_numeric(df.cik)

# Start by selecting only the 'cik' and 'cusip6' columns.
df = df[['cik', 'cusip6']]

# Group by 'cusip6' and 'cik', then count the occurrences of each 'cik' within each 'cusip6' group.
counts = df.groupby(['cusip6', 'cik']).size().reset_index(name='counts')

# Sort within each 'cusip6' group by 'counts' in descending order, so the most frequent 'cik' comes first.
# Then, drop duplicates to keep only the most frequent 'cik' for each 'cusip6'.
most_frequent = counts.sort_values(['cusip6', 'counts'], ascending=[True, False])\
    .drop_duplicates('cusip6').reset_index(drop=True)

# Save the resulting DataFrame to a CSV file.
most_frequent.to_csv('17_data_new/cik-cusip-maps.csv', index=False)
