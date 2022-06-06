import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from global_env import CONFIG_DIR

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    nse_symbol_urls = [
        'https://archives.nseindia.com/content/equities/EQUITY_L.csv',
        'https://archives.nseindia.com/content/indices/ind_nifty50list.csv',
        'https://archives.nseindia.com/content/indices/ind_nifty100list.csv',
        'https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv',
        'https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv',
        'https://archives.nseindia.com/content/indices/ind_nifty500list.csv',
        'https://archives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv',
    ]

    for url in nse_symbol_urls:
        print('Downloading', f'{os.path.basename(url)}', end=' ... ')
        df = pd.read_csv(url)
        df.to_csv(CONFIG_DIR + f'/{os.path.basename(url)}', index=False)
        print('Done, shape:', df.shape)

    print('Downloading nse_symbolchanges.csv', end=' ... ')
    nse_symbol_changes_url = 'https://archives.nseindia.com/content/equities/symbolchange.csv'
    df = pd.read_csv(nse_symbol_changes_url, encoding='cp1252')
    df.to_csv(CONFIG_DIR + '/nse_symbol_changes.csv', index=False)
    print('Done, shape:', df.shape)

    # TO DO: BSE_CODES