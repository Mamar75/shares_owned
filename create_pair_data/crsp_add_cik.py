# python '18_shares_owned/create_pair_data/crsp_add_cik.py'

import pandas as pd

# Import
crsp_data = pd.read_csv('17_data_new/crsp_data.csv')
crsp_data = crsp_data[['PERMNO', 'CUSIP', 'Ticker', 'PERMCO',
                       'NAICS','YYYYMMDD', 'DlyPrc', 'DlyCap',
                       'DlyVol', 'ShrOut']]
cik_cusip_data = pd.read_csv('17_data_new/cik-cusip-maps.csv')

# Create a dictionary
cik_cusip_data = dict(zip(cik_cusip_data['cusip6'], cik_cusip_data['cik']))

# Create a cik column
crsp_data['CUSIP6'] = crsp_data['CUSIP'].str[:6]
crsp_data['CIK'] = crsp_data['CUSIP6'].map(cik_cusip_data)
crsp_data = crsp_data.dropna(subset=['CIK'])

# Place cik column behind cusip column
cols = [c for c in crsp_data.columns if c not in ['CIK', 'CUSIP6']]
target_index = cols.index('CUSIP')
cols.insert(target_index + 1, 'CUSIP6')
cols.insert(target_index + 2, 'CIK')
crsp_data = crsp_data[cols]

crsp_data.to_csv('17_data_new/short_crsp_data.csv', index=False)










