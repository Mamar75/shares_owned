# python '18_shares_owned/dl_idx.pl(1).py'

import csv
import io
import pandas as pd
import requests
from datetime import datetime
import os


user_agent = {"User-agent": "Mozilla/5.0"}


def most_recent_year(csv_file, default_year):
    """
    This function finds the most recent year in the current full_index.csv file.
    Argument 1: full_index.csv file path.
    Argument 2: default date if full_index.csv doesn't exist.
    """
    try:
        df = pd.read_csv(csv_file, parse_dates=['date'], encoding='latin1')

        if df.empty:
            return default_year

        most_recent_date = df['date'].max()

        if pd.isnull(most_recent_date):
            return default_year

        return int(most_recent_date.strftime('%Y'))

    except FileNotFoundError:
        return default_year


def update_csv_with_new_data(existing_file_path, new_data_io):
    """
    This function will update the existing fil full_index.cdv.
    If full_index.csv doesn't exist, it is created.
    Argument 1: file_path of full_index.csv.
    Argument 2: fictive file with new data.
    new_data_io should be a dataset filled with the most recent
    SEC filings. This function compares if the filing in
    new_data_io are already present in the existing file, if
    not these are added.
    """
    existing_data = set()

    if os.path.exists(existing_file_path):
        with open(existing_file_path, mode='r', newline='', encoding='latin1') as file:
            reader = csv.reader(file)
            existing_data = set(tuple(row) for row in reader)

    new_data_io.seek(0)

    reader = csv.reader(new_data_io)
    new_data_set = set(tuple(row) for row in reader)

    data_to_add = [row for row in new_data_set if row not in existing_data]

    with open(existing_file_path, mode='a', newline='', encoding='latin1') as file:
        writer = csv.writer(file)

        if file.tell() == 0 and any(data_to_add):
            writer.writerow(["cik", "comnam", "form", "date", "url"])  # Adjust headers as needed
        writer.writerows(data_to_add)


# (1) master.idx file is created or overwritten with the current year sec filing indexes.
# (2) A fictive csv file is created where the .txt file present in master.idx are written.
# (3) Last year sec file indexes are added to the current full_index.csv if not already present.
if __name__ == "__main__":

    with open(f"17_data_new\master.idx", "wb") as f:  # (1)

        start = most_recent_year(r"17_data_new\full_index.csv", 1993)
        end = datetime.now().year
        for year in range(start, end + 1):
            for q in range(1, 5):
                print(year, q)
                content = requests.get(
                    f"https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{q}/master.idx",
                    headers=user_agent,
                ).content
                f.write(content)

    fictive_file = io.StringIO()  # (2)
    wr = csv.writer(fictive_file)
    wr.writerow(["cik", "comnam", "form", "date", "url"])

    with open(r"17_data_new\master.idx", "r", encoding="latin1") as f:
        for r in f:
            if ".txt" in r:
                wr.writerow(r.strip().split("|"))

    update_csv_with_new_data(r'17_data_new\full_index.csv',fictive_file)  # (3)