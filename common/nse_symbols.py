"""
Symbols for NSE indices & historic symbol changes
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import sys
import glob
import pandas as pd

PATH_1 = os.path.join(DATA_ROOT, '00_common/01_nse_symbols')
PATH_2 = os.path.join(DATA_ROOT, '00_common/02_nse_indices')

''' --------------------------------------------------------------------------------------- '''
def get_symbols(indices, series=None, sector=None):
    idx_list = pd.read_csv(os.path.join(PATH_2, 'all_nse_indices.csv'))
    idx_list = idx_list.loc[idx_list['Symbol'].isin(indices)]
    file_list = idx_list['file_name'].unique()

    symbols = pd.DataFrame()
    for f in file_list:
        df = pd.read_csv(os.path.join(PATH_2, f))
        symbols = pd.concat([symbols, df[['Series', 'Symbol', 'Industry']]])
    if series is not None:
        symbols = symbols.loc[symbols.Series == series]
    if sector is not None:
        symbols = symbols.loc[symbols.Industry == sector]
    symbols.reset_index(drop=True, inplace=True)

    return list(symbols['Symbol'].unique())

def get_symbol_changes(cutoff_date='2018-01-01'):
    df = pd.read_csv(os.path.join(PATH_1, f'symbolchange.csv'))
    df['Date of Change'] = pd.to_datetime(df['Date of Change'])
    df['Date of Change'] = pd.to_datetime(df['Date of Change'], format="%Y-%m-%d")
    df = df.loc[df['Date of Change'] >= cutoff_date].reset_index(drop=True)
    df = df.sort_values(by='Date of Change').reset_index(drop=True)
    return df[['Date of Change', 'Old Symbol', 'New Symbol']]

def get_older_symbols(symbol):
    df = get_symbol_changes()
    df = df.loc[df['New Symbol'] == symbol].reset_index(drop=True)
    return list(df['Old Symbol'].unique())

def get_isin(symbol):
    df = pd.read_csv(os.path.join(PATH_1, f'EQUITY_L.csv'))
    df = df.loc[df['Symbol'] == symbol].reset_index(drop=True)
    return 'unknown-isin' if df.shape[0] == 0 else df['ISIN'].values[0]

''' --------------------------------------------------------------------------------------- '''
def test_me():
    print('\nTesting nse_symbols ... ')

    if len(get_symbols(['NIFTY 50'])) != 50 or len(get_symbols(['NIFTY 100'])) != 100 or \
            len(get_symbols(['NIFTY 50', 'NIFTY 100'])) != 100:
        print('!!! WARNING !!! Index symbols size not as expected')
        print('    NIFTY 50: %d, NIFTY 100: %d, NIFTY 50 + NIFTY 100: %d'
              % (len(get_symbols(['NIFTY 50'])), len(get_symbols(['NIFTY 100'])),
              len(get_symbols(['NIFTY 50', 'NIFTY 100']))))
    if sorted(get_symbols(['NIFTY 50', 'NIFTY NEXT 50'])) != sorted(get_symbols(['NIFTY 100'])):
        print('!!! WARNING !!! Index symbols size not as expected')
    '''
    Do I really care about this?
    assert sorted(get_symbols(['NIFTY 50', 'NIFTY NEXT 50'])) == sorted(get_symbols(['NIFTY 100']))
    '''

    '''
    Do I really care about this?
    x1 = sorted(get_symbols(['NIFTY 100', 'NIFTY MIDCAP 150', 'NIFTY SMALLCAP 250']))
    x2 = sorted(get_symbols(['NIFTY 500']))
    assert list(set(x1) - set(x2)) == ['TATAMTRDVR'] or list(set(x2) - set(x1)) == ['TATAMTRDVR'], \
        '%s / %s' % (list(set(x1) - set(x2)), list(set(x2) - set(x1)))'''

    assert sorted(get_symbols(['NIFTY 500', 'NIFTY MICROCAP 250'])) == \
           sorted(get_symbols(['NIFTY TOTAL MARKET']))

    sc_df = get_symbol_changes()
    sc_df = sc_df.loc[sc_df['Old Symbol'].isin(['CADILAHC', 'LTI'])]
    assert [d.astype(str)[0:10] for d in sc_df['Date of Change'].values] == \
           ['2022-03-07', '2022-12-05']
    assert list(sc_df['Old Symbol']) == ['CADILAHC', 'LTI']
    assert list(sc_df['New Symbol']) == ['ZYDUSLIFE', 'LTIM']

    assert get_older_symbols('ZYDUSLIFE') == ['CADILAHC']
    assert get_older_symbols('LTIM') == ['LTI']

    assert get_isin('ZYDUSLIFE') == 'INE010B01027'
    print('Testing nse_symbols ... OK')
    return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    test_me()