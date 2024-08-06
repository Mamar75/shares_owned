import pandas as pd
from datetime import datetime


def date_formatting(date):
    """
    December 30, 1993 -> 12-30-1993
    """
    date_formats = [
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    for date_format in date_formats:
        try:
            parsed_date = datetime.strptime(date, date_format)
            formatted_date = parsed_date.strftime('%m-%d-%Y')
            return formatted_date
        except ValueError:
            return 'no match'


def sample_date(date, date_issue):
    """
    If the argument date is in this format 'December 30, 1993' then it returns 12-30-1993, if not
    the argument date_issue is returned. This is done by checking if the function pd.to_datetime()
    returns a date and not NAT. This will be case each time 'no match' is encountered. And in really
    rare cases, misspelling of the transaction date like 'April 12, 1006' for 'April 12, 2006' makes
    the conversion to date type impossible.
    """
    date_out = date_formatting(date)
    date_out = pd.to_datetime(date_out, format='%m-%d-%Y', errors='coerce')
    if pd.isna(date_out):
        return date_issue
    else:
        date_out = date_out.strftime('%m-%d-%Y')
        return date_out


if __name__ == '__main__':
    # python '18_shares_owned/create_shares_owned/date_formatting.py'
    shares_owned = pd.read_csv('17_data_new/shares_owned')
    shares_owned['date_sample'] = shares_owned.apply(lambda row: sample_date(row['date_transaction'],
                                                                             row['date_issue']), axis=1)
    # Places date_sample column behind date_transaction
    index_of_date_transaction = shares_owned.columns.get_loc('date_transaction')
    date_sample_column = shares_owned.pop('date_sample')
    shares_owned.insert(index_of_date_transaction + 1, 'date_sample', date_sample_column)
    shares_owned.to_csv('17_data_new/shares_owned')

