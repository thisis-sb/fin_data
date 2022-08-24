import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from settings import CONFIG_DIR

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    # 1. NSE symbols & indices
    nse_symbol_urls = [
        'https://archives.nseindia.com/content/equities/EQUITY_L.csv',
        'https://archives.nseindia.com/content/indices/ind_nifty50list.csv',
        'https://archives.nseindia.com/content/indices/ind_nifty100list.csv',
        'https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv',
        'https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv',
        'https://archives.nseindia.com/content/indices/ind_nifty500list.csv',
        'https://archives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv',
        'https://archives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv'
    ]

    for url in nse_symbol_urls:
        print('Downloading', f'{os.path.basename(url)}', end=' ... ')
        df = pd.read_csv(url)
        df.to_csv(CONFIG_DIR + f'/02_nse_symbols/{os.path.basename(url)}', index=False)
        print('Done, shape:', df.shape)

    # 2. NSE symbol changes
    print('Downloading nse_symbolchanges.csv', end=' ... ')
    nse_symbol_changes_url = 'https://archives.nseindia.com/content/equities/symbolchange.csv'
    df = pd.read_csv(nse_symbol_changes_url, encoding='cp1252')
    df.to_csv(CONFIG_DIR + '/02_nse_symbols/nse_symbol_changes.csv', index=False)
    print('Done, shape:', df.shape)

    # 3. FO market lots
    print('Downloading fo_mktlots.csv', end=' ... ')
    nse_symbol_changes_url = 'https://archives.nseindia.com/content/fo/fo_mktlots.csv'
    df = pd.read_csv(nse_symbol_changes_url, encoding='cp1252')
    df.to_csv(CONFIG_DIR + '/02_nse_symbols/fo_mktlots.csv', index=False)
    print('Done, shape:', df.shape)

    # 3. ------- TO DO: BSE_CODES -------
    # -----------------------------------

    # 4. Prepare list of symbol & index membership
    print('Preparing nse_symbols_master.csv', end=' ... ')
    equity_l_df = pd.read_csv(os.path.join(CONFIG_DIR, '02_nse_symbols/EQUITY_L.csv'))
    equity_l_df.rename(columns=lambda x: x.strip(), inplace=True)
    equity_l_df = equity_l_df[['SYMBOL', 'NAME OF COMPANY', 'SERIES', 'ISIN NUMBER']]
    equity_l_df.rename(columns={'SYMBOL': 'Symbol',
                                'ISIN NUMBER': 'ISIN',
                                'SERIES': 'Series',
                                'NAME OF COMPANY': 'Company Name'
                                }, inplace=True)

    indices = {'NIFTY 50': 'ind_nifty50list',
               'NIFTY 100': 'ind_nifty100list',
               'MIDCAP 150': 'ind_niftymidcap150list',
               'SMALLCAP 250': 'ind_niftysmallcap250list',
               'MICROCAP 250': 'ind_niftymicrocap250_list',
               'MY_CUSTOM_INDEX': 'ind_my_custom_index'
               }
    indices_master = pd.DataFrame()
    for index_name in list(indices.keys()):
        df = pd.read_csv(os.path.join(CONFIG_DIR, f'02_nse_symbols/{indices[index_name]}.csv'))
        df.rename(columns=lambda x: x.strip(), inplace=True)
        df.rename(columns={'ISIN Code': 'ISIN'}, inplace=True)
        df['index_name'] = index_name
        if indices_master.shape[0] > 0:
            df = df[~df['ISIN'].isin(indices_master['ISIN'])]
            df.reset_index(drop=True, inplace=True)
        indices_master = pd.concat([indices_master, df])

    indices_master.drop(columns=['Company Name'], inplace=True)
    df = pd.merge(equity_l_df, indices_master, on=['Symbol', 'ISIN', 'Series'], how='left')
    df.to_csv(os.path.join(CONFIG_DIR, '02_nse_symbols/nse_symbols_master.csv'), index=False)
    print('Done, shape:', df.shape)


