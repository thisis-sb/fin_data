# --------------------------------------------------------------------------------------------
# Symbols for NSE indices & historic symbol changes
# --------------------------------------------------------------------------------------------

import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from settings import CONFIG_DIR

MODULE = '02_nse_symbols'

def get_symbols(file_list, filter='EQ'):
    symbols = pd.DataFrame()
    for f in file_list:
        df = pd.read_csv(CONFIG_DIR + f'/{MODULE}/{f}.csv')
        if 'Group' in df.columns:
            df.rename(columns={'Group':'Series'}, inplace=True)
        symbols = pd.concat([symbols, df[['Series', 'Symbol']]])
    if filter is not None:
        symbols = symbols.loc[symbols.Series == filter].reset_index(drop=True)
    return symbols['Symbol'].unique()

def get_symbol_changes():
    df = pd.read_csv(CONFIG_DIR + f'/{MODULE}/nse_symbol_changes.csv')
    df.columns = df.columns.str.replace(' ', '')
    df['Date'] = pd.to_datetime(df['SM_APPLICABLE_FROM'])
    df = df.loc[df['Date'] >= '2019-01-01'].reset_index(drop=True)
    df = df.sort_values(by='Date').reset_index(drop=True)
    df.rename(columns={'SM_KEY_SYMBOL':'old', 'SM_NEW_SYMBOL':'new'}, inplace=True)
    return df[['Date', 'old', 'new']]

def get_older_symbols(symbol):
    sc_df = get_symbol_changes()
    sc_df = sc_df.loc[sc_df['new'] == symbol].reset_index(drop=True)
    return list(sc_df['old'].unique())

def get_isin(symbol):
    df = pd.read_csv(CONFIG_DIR + f'/{MODULE}/EQUITY_L.csv')
    df = df.loc[df['SYMBOL'] == symbol].reset_index(drop=True)
    assert df.shape[0] == 1
    return df[' ISIN NUMBER'].values[0]


# ----------------------------------------------------------------------------------------------
if __name__ == '__main__':
    print('Testing nse_config ...')

    print(len(get_symbols(['ind_nifty50list', 'ind_nifty100list'])))

    sc_df = get_symbol_changes()
    print(sc_df.loc[sc_df['old'].isin(['CADILAHC', 'WABCOINDIA'])].to_string(index=False))

    old_syms = ['CADILAHC', 'WABCOINDIA']
    new_syms = ['ZYDUSLIFE', 'ZFCVINDIA']
    print('Old symbols:::')
    [print(ns, ':', get_older_symbols(ns)) for ns in new_syms]

    assert get_isin('ZYDUSLIFE') == 'INE010B01027'
    print('All OK')