# python '18_shares_owned/all_cik.py' '17_data_new\13D'
import argparse
import csv
import re
import sys
from collections import *
from glob import glob
from multiprocessing import Pool
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('files')
parser.add_argument('--debug', action='store_true')

args = parser.parse_args()

pat = re.compile(
    '[\( >]*[0-9A-Z]{1}[0-9]{3}[0-9A-Za-z]{2}[- ]*[0-9]{0,2}[- ]*[0-9]{0,1}[\) \n<]*'
)
w = re.compile('\w+')


def parse(file):
    with open(file, 'r') as f:
        lines = f.readlines()

    record = 0
    cik = None
    for line in lines:
        if 'SUBJECT COMPANY' in line:
            record = 1
        if 'CENTRAL INDEX KEY' in line and record == 1:
            cik = line.split('\t\t\t')[-1].strip()
            break
    record = 0
    cik_owner = None
    for line in lines:
        if 'FILED BY' in line:
            record = 1
        if 'CENTRAL INDEX KEY' in line and record == 1:
            cik_owner = line.split('\t\t\t')[-1].strip()
            break

    cusips = []
    record = 0
    for line in lines:
        if '<DOCUMENT>' in line:  # lines are after the document preamble
            record = 1
        if record == 1:
            if 'IRS' not in line and 'I.R.S' not in line:
                fd = pat.findall(line)
                if fd:
                    cusip = fd[0].strip().strip('<>')
                    if args.debug:
                        print('INFO: added --- ', line, " --- extracted [",
                              cusip, "]")
                    cusips.append(cusip)
    if len(cusips) == 0:
        cusip = None
    else:
        cusip = Counter(cusips).most_common()[0][0]
        cusip = ''.join(w.findall(cusip))
    if args.debug:
        print(cusip)

    return [file, cik, cik_owner, cusip]


def main():
    if args.debug:
        path = Path(args.files)
        if path.exists():
            print(parse(args.files))
        else:
            raise ValueError("provide a single file to debug ...")
        return

    i = 0
    with Pool(5) as p:
        with open(args.files + '.csv', 'w') as w:
            wr = csv.writer(w)
            all_files = glob(args.files + '/*/*')
            for res in p.imap(parse, all_files, chunksize=100):
                print(f'{i} on {len(all_files)}.')
                i += 1
                wr.writerow(res)


if __name__ == '__main__':
    main()