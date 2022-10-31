import logging
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

log = logging.getLogger("sp500")
logging.basicConfig(level=logging.INFO)


def get_sp500ListFromWikipedia():
    """
    Scrapes the wikipedia page for the list of constituents
    :Returns: a dataframe
    """
    data = requests.get(url).text
    soup = BeautifulSoup(data, 'html.parser')
    table = soup.find_all('table', {'id': 'constituents'})

    table_body = table[0].find_all('tbody')

    rows = table_body[0].find_all('tr')
    data = []
    headers = ['symbol', 'security', 'sec_filings', 'gics_sector', 'sub_industry', 'headquarters', 'date_added', 'cik',
               'founded']
    for tr in rows:
        cols = tr.findAll('td')
        row = []
        if cols:
            for a in cols:
                row.append(a.text.strip())
        if row:
            data.append(row)
    df = pd.DataFrame(data, columns=headers)
    return df


def get_size(filename):
    """
    size of the file created
    :return: 0 if nothing else file
    """
    if os.path.isfile(filename):
        if os.path.isfile(filename):
            st = os.stat(filename)
            return st.st_size
    return 0


def is_valid_file(file_name):
    return os.path.isfile(file_name) and get_size(file_name) > 0


def export_df(data_df, file_name, index_required=True):
    try:
        data_df.to_csv(file_name, index=index_required, encoding='utf-8-sig')
        return True
    except PermissionError:
        log.error("unable to access existing file {}".format(file_name))
        return False
    except Exception as e:
        log.error(e)
        return False


def get_file_name(file_name, timestamp=True, file_ext='.csv'):
    full_file = file_name + "_" + get_date_stamp() + file_ext if timestamp else file_name + file_ext
    return full_file


def get_date_stamp():
    return datetime.today().strftime('%Y%m%d')


def create_sp500_file():
    """
    Main process to extact the data
    :return: True or False if created
    """
    df = get_sp500ListFromWikipedia()
    file = get_file_name('sp500list', False)
    export_df(df, file, False)
    is_valid = is_valid_file(file)
    if not is_valid:
        log.error("file [{}] does NOT exist!".format(file))
        return False
    else:
        log.info("file [{}] has been created!".format(file))
        return True


if __name__ == '__main__':
    create_sp500_file()
