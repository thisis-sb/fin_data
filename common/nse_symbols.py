"""
Symbols for NSE indices & historic symbol changes
"""
''' --------------------------------------------------------------------------------------- '''

import os
import sys
import glob
import pandas as pd

CONFIG_ROOT = os.getenv('CONFIG_ROOT')
PATH_0 = os.path.join(CONFIG_ROOT, '00_manual')
PATH_1 = os.path.join(os.getenv('CONFIG_ROOT'), '01_nse_symbols')
PATH_2 = os.path.join(os.getenv('CONFIG_ROOT'), '02_nse_indices')

''' --------------------------------------------------------------------------------------- '''
def get_symbols(indices, series=None):
    idx_list = pd.read_excel(os.path.join(PATH_0, '00_meta_data.xlsx'), sheet_name='indices')
    idx_list = idx_list.loc[idx_list['Symbol'].isin(indices)].dropna().reset_index(drop=True)
    file_list = idx_list['File Name'].unique()

    symbols = pd.DataFrame()
    for f in file_list:
        df = pd.read_csv(os.path.join(PATH_2, f))
        if 'Group' in df.columns:
            df.rename(columns={'Group':'Series'}, inplace=True)
        symbols = pd.concat([symbols, df[['Series', 'Symbol']]])
    if series is not None:
        symbols = symbols.loc[symbols.Series == series].reset_index(drop=True)
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
    assert df.shape[0] == 1
    return df['ISIN'].values[0]

def index_filename(code=0):
    idx_list = pd.read_excel(os.path.join(PATH_0, '00_meta_data.xlsx'), sheet_name='indices')
    idx_list = idx_list.dropna().reset_index(drop=True)
    idx_list.reset_index(inplace=True)
    idx_list['index'] = idx_list['index'] + 1
    idx_map = dict(zip(idx_list['index'], idx_list['Symbol']))
    return idx_map if code == 0 else idx_map[code]

def test_me():
    print('\nTesting nse_symbols ... ')

    if len(get_symbols(['NIFTY 50'])) != 50 or len(get_symbols(['NIFTY 100'])) != 100 or \
            len(get_symbols(['NIFTY 50', 'NIFTY 100'])) != 100:
        print('!!! WARNING !!! Index symbols size not as expected')
        print('    NIFTY 50: %d, NIFTY 100: %d, NIFTY 50 + NIFTY 100: %d'
              % (len(get_symbols(['NIFTY 50'])), len(get_symbols(['NIFTY 100'])),
              len(get_symbols(['NIFTY 50', 'NIFTY 100']))))
    assert sorted(get_symbols(['NIFTY 50', 'NIFTY NEXT 50'])) == sorted(get_symbols(['NIFTY 100']))

    x1 = sorted(get_symbols(['NIFTY 100', 'NIFTY MIDCAP 150', 'NIFTY SMALLCAP 250']))
    x2 = sorted(get_symbols(['NIFTY 500']))
    assert list(set(x1) - set(x2)) == ['TATAMTRDVR'] or list(set(x2) - set(x1)) == ['TATAMTRDVR'], \
        '%s / %s' % (list(set(x1) - set(x2)), list(set(x2) - set(x1)))
    assert sorted(get_symbols(['NIFTY 500', 'NIFTY MICROCAP 250'])) == sorted(
        get_symbols(['NIFTY TOTAL MARKET']))

    sc_df = get_symbol_changes()
    sc_df = sc_df.loc[sc_df['Old Symbol'].isin(['CADILAHC', 'LTI'])]
    # print(sc_df)
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

    idxs = index_filename(0)
    print('\nAll indices with codes:')
    print('\n'.join(['%2s    %s' % (c, idxs[c]) for c in idxs.keys()]))

    assert (index_filename(1) == 'NIFTY 50') and (index_filename(20) == 'NIFTY HEALTHCARE')