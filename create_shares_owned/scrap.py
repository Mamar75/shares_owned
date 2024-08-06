from filing_parser import FileSec
import pandas as pd
import time
from multiprocessing import Pool
from tqdm import tqdm


# Define a function that will parse each file.
def parse(file):

    FileSec1 = FileSec(file)
    return {
        "file_path": file,
        "file_type": FileSec1.find_file_type(),
        "date_issue": FileSec1.find_issue_date(),
        "date_transaction": FileSec1.find_transaction_date(),
        "cusips": FileSec1.find_cusip(),
        "company": FileSec1.find_company_name(),
        "cik": FileSec1.find_company_cik(),
        "owner": FileSec1.find_owner_name(),
        "cik_owner": FileSec1.find_owner_cik(),
        "shares_agg": FileSec1.find_nb_shares_agg(),
        "shares_sole_vote": FileSec1.find_nb_shares_sole_voting(),
        "shares_shared_vote": FileSec1.find_nb_shares_shared_voting(),
        "shares_sole_dispositive": FileSec1.find_nb_shares_sole_dispositive(),
        "shares_shared_dispositive": FileSec1.find_nb_shares_shared_dispositive(),
        "shares_percentage": FileSec1.find_percentage_owned()
    }


if __name__ == "__main__":

    start = time.time()
    public_data = pd.read_csv('17_data_new/public_file_data')
    # public_data = public_data.iloc[0:5000]
    files = public_data['file_path'].to_list()

    with Pool(4) as p:
        # results = p.map(parse, files)
        results = list(tqdm(p.imap_unordered(parse, files), total=len(files)))

        columns = ['file_path', 'file_type', 'date_issue', 'date_transaction',
                   'cusips', 'company', 'cik', 'owner', 'cik_owner',
                   'shares_agg', 'shares_sole_vote', 'shares_shared_vote',
                   'shares_sole_dispositive', 'shares_shared_dispositive', 'shares_percentage']
        df = pd.DataFrame(results, columns=columns)

    df.to_csv('17_data_new/shares_owned')

    time = time.time() - start
    print(f'It took {time} seconds.')

