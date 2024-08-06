import pandas as pd

# Load the data
compustat_data = pd.read_csv('../17_data_new/compustat_quaterly.csv')
pairs_data = pd.read_csv('../17_data_new/pair_data.csv')

# Merge the dataframes using a left join
merged_data = pd.merge(pairs_data, compustat_data, left_on=['date_sample', 'cusips'], right_on=['datadate', 'cusip'], how='left')

# Columns to forward fill
columns_to_fill = ["gvkey", "datadate", "fyearq", "fqtr", "indfmt", "consol", "popsrc", "datafmt",
                   "tic", "cusip", "conm", "curcdq", "datacqtr", "datafqtr", "actq", "ceqq", "cshoq",
                   "lctq", "teqq", "uceqq", "exchg", "cik", "costat", "mkvaltq", "ggroup", "gind",
                   "gsector", "gsubind"]

# Apply forward fill to the specified columns
merged_data[columns_to_fill] = merged_data[columns_to_fill].fillna(method='ffill')

# Export the DataFrame to CSV
merged_data.to_csv('../17_data_new/pair_data_comp.csv', index=False)

print("The CSV file has been saved successfully.")

