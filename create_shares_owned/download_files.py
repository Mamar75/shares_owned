# python '18_shares_owned/dl.py' '13G' '17_data_new/13G'
# python '18_shares_owned/dl.py' '13D' '17_data_new/13D'
import argparse
import csv
from pathlib import Path
import time
import requests


def ensure_rate_limit(last_request_time, request_count, max_requests_per_second=10):
    if request_count >= max_requests_per_second:
        elapsed = time.time() - last_request_time
        sleep_time = max(0, 1 - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)
        return time.time(), 0
    return last_request_time, request_count + 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filing", type=str)
    parser.add_argument("folder", type=str)

    user_agent = {"User-agent": "Mozilla/5.0"}

    args = parser.parse_args()
    filing = args.filing
    folder = args.folder

    to_dl = []
    files_to_download = []
    files_already_present = 0

    with open(r"17_data_new\full_index.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if filing in row["form"]:
                to_dl.append(row)

    print("start to download")

    # First Loop: Check for existing files and count them
    for row in to_dl:
        cik = row["cik"].strip()
        date = row["date"].strip()
        year = row["date"].split("-")[0].strip()
        month = row["date"].split("-")[1].strip()
        url = row["url"].strip()
        accession = url.split(".")[0].split("-")[-1]
        file_path = Path(f"{folder}/{year}_{month}/{cik}_{date}_{accession}.txt")

        if not file_path.exists():
            files_to_download.append(row)  # Add to download list if not exists
        else:
            files_already_present += 1  # Increment count for already present files

    # Display counts
    print(f"Total filings to process: {len(to_dl)}")
    print(f"Files already present: {files_already_present}")
    print(f"Files to download: {len(files_to_download)}")

    # Second Loop: Download files, applying rate limit
    last_request_time = time.time()
    request_count = 0

    for n, row in enumerate(files_to_download):
        last_request_time, request_count = ensure_rate_limit(last_request_time, request_count)

        print(f"Downloading {n + 1} out of {len(files_to_download)}")

        cik = row["cik"].strip()
        date = row["date"].strip()
        year = row["date"].split("-")[0].strip()
        month = row["date"].split("-")[1].strip()
        url = row["url"].strip()
        accession = url.split(".")[0].split("-")[-1]
        Path(f"./{folder}/{year}_{month}").mkdir(parents=True, exist_ok=True)
        file_path = f"./{folder}/{year}_{month}/{cik}_{date}_{accession}.txt"

        try:
            txt = requests.get(
                f"https://www.sec.gov/Archives/{url}", headers=user_agent, timeout=60
            ).text
            with open(file_path, "w", errors="ignore") as f:
                f.write(txt)
        except:
            print(f"{cik}, {date} failed to download")