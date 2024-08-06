# python '18_shares_owned/0_launch.py'
import subprocess
import time
import sys

# Record the start time
start_time = time.time()

list_argument = sys.argv[1:]

# Run create_index_file.py
# This creates 'full_index.csv' with the indexes of all SEC digitalized filings.
if len(list_argument) == 0 or 'create_index_file.py' in list_argument:
    print('create_index_file.py')
    command = ['python', '18_shares_owned/create_shares_owned/create_index_file.py']
    subprocess.run(command)

# Run dl.py
# This download all specific filings using the 'full_index.csv' to creat the appropriate url.
# Argument 1: filing type that should be downloaded.
# Argument 2: folder in which to download the filings.
if len(list_argument) == 0 or 'download_files.py' in list_argument:
    print('start download_files.py for 13D')
    command = ['python', '18_shares_owned/create_shares_owned/download_files.py', '13D', '17_data_new/13D']
    subprocess.run(command)
    print('start download_files.py for 13G')
    command = ['python', '18_shares_owned/create_shares_owned/download_files.py', '13G', '17_data_new/13G']
    subprocess.run(command)

# Run all_cik
# This creates a .csv with the cusip, cik and cik owner.
# Argument 1: the folders in which the filings are.
if len(list_argument) == 0 or 'cusip_parser.py' in list_argument:
    print('start all_cik.py for 13D')
    command = ['python', '18_shares_owned/create_shares_owned/cusip_parser.py', '17_data_new/13D']
    subprocess.run(command)
    print('start all_cik.py for 13G')
    command = ['python', '18_shares_owned/create_shares_owned/cusip_parser.py', '17_data_new/13G']
    subprocess.run(command)

# Run post_proc.py
# This creates cik-cusip-maps.csv
# Argument 1: .csv filings with cik and cuisp.
# Argument 2: .csv filings with cik and cuisp.
# Argument 3: ...
if len(list_argument) == 0 or 'create_cusip_mapping.py' in list_argument:
    print('start post_proc.py')
    command = ['python', '18_shares_owned/create_shares_owned/create_cusip_mapping.py', '17_data_new/13D.csv',
               '17_data_new/13G.csv']
    subprocess.run(command)

# Find all public companies owner
# Select all cik owner also existing as owned companies
if len(list_argument) == 0 or 'find_public.py' in list_argument:
    print('start find_public.py')
    command = ['python', '18_shares_owned/create_shares_owned/find_public.py', '17_data_new/13D.csv',
               '17_data_new/13G.csv']
    subprocess.run(command)

# Record the end time
end_time = time.time()

# Calculate the elapsed time
elapsed_time = (end_time - start_time)/60
print(f"Execution time: {elapsed_time} minutes")
