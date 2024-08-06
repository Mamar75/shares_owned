import pandas as pd
import os


def cusip_crsp_finder(data, cusip):
    filtered_data = data[data['CUSIP9'] == cusip]

    return filtered_data


def filings_cik_finder(path, cik):
    data = pd.read_csv(path, names=['f', 'cik', 'cik_owner', 'cusip'])
    filtered_data = data[data['cik'] == cik]
    return filtered_data



if __name__ == '__main__':
    crsp_data = pd.read_csv('../17_data_new/crsp_data.csv')
    data = cusip_crsp_finder(crsp_data, '343496105')
    data.to_csv('../17_data_new/flowers_food.csv')

    # data = filings_cik_finder('../17_data_new/13D.csv', 826227)
    # print(data)
    # crsp_data = pd.read_csv('../17_data_new/crsp_data.csv')
    # data = cusip_crsp_finder(crsp_data, '487256109')
    # data.to_csv('../17_data_new/keebler_food.csv')