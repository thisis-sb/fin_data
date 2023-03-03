"""
Symbols for NSE indices & historic symbol changes
"""
''' --------------------------------------------------------------------------------------- '''

import os
import sys
import glob
import pandas as pd

CONFIG_ROOT = os.getenv('CONFIG_ROOT')
PATH_1 = os.path.join(os.getenv('CONFIG_ROOT'), '01_nse_symbols')
PATH_2 = os.path.join(os.getenv('CONFIG_ROOT'), '02_nse_indices')

''' --------------------------------------------------------------------------------------- '''
def get_symbols(file_list, series=None):
    symbols = pd.DataFrame()
    for f in file_list:
        df = pd.read_csv(os.path.join(PATH_2, f'{f}.csv'))
        if 'Group' in df.columns:
            df.rename(columns={'Group':'Series'}, inplace=True)
        symbols = pd.concat([symbols, df[['Series', 'Symbol']]])
    if series is not None:
        symbols = symbols.loc[symbols.Series == series].reset_index(drop=True)
    return symbols['Symbol'].unique()

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
    indices = {1: 'ind_nifty50list',
               2: 'ind_niftynext50list',
               3: 'ind_niftymidcap150list',
               4: 'ind_niftysmallcap250list',
               5: 'ind_niftymicrocap250_list',
               6: 'ind_nifty100list',
               7: 'ind_nifty200list',
               8: 'ind_nifty500list',
               9: 'ind_niftytotalmarket_list'}
    for idx, f in enumerate(glob.glob(os.path.join(PATH_2, '*.csv'))):
        ff = os.path.basename(f).split('.')[0]
        if ff not in indices.values():
            indices[list(indices.keys())[-1] + 1] = os.path.basename(f).split('.')[0]
    return indices if code == 0 else indices[code]

# ----------------------------------------------------------------------------------------------
if __name__ == '__main__':
    print('Testing nse_config ... ', end='')

    assert len(get_symbols(['ind_nifty50list'])) == 50
    assert len(get_symbols(['ind_nifty100list'])) == 100
    assert len(get_symbols(['ind_nifty50list', 'ind_nifty100list'])) == 100

    sc_df = get_symbol_changes()
    sc_df = sc_df.loc[sc_df['Old Symbol'].isin(['CADILAHC', 'LTI'])]
    print(sc_df)
    assert [d.astype(str)[0:10] for d in sc_df['Date of Change'].values] == \
           ['2022-03-07', '2022-12-05']

    assert get_older_symbols('ZYDUSLIFE') == ['CADILAHC']
    assert get_older_symbols('LTIM') == ['LTI']

    assert get_isin('ZYDUSLIFE') == 'INE010B01027'
    print('All OK')

    idxs = index_filename(0)
    print('\nAll indices with codes:')
    print('\n'.join(['%2s    %s' % (c, idxs[c]) for c in idxs.keys()]))

    assert (index_filename(1) == 'ind_nifty50list') and\
           (index_filename(15) == 'sect_ind_NIFTY_HEALTHCARE_INDEX')