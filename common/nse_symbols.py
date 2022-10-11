"""
Symbols for NSE indices & historic symbol changes
"""

''' --------------------------------------------------------------------------------------- '''
import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from settings import CONFIG_DIR, CUTOFF_DATE

MODULE = '01_nse_symbols'

''' --------------------------------------------------------------------------------------- '''
def get_symbols(file_list, series=None):
    symbols = pd.DataFrame()
    for f in file_list:
        df = pd.read_csv(CONFIG_DIR + f'/{MODULE}/{f}.csv')
        if 'Group' in df.columns:
            df.rename(columns={'Group':'Series'}, inplace=True)
        symbols = pd.concat([symbols, df[['Series', 'Symbol']]])
    if series is not None:
        symbols = symbols.loc[symbols.Series == series].reset_index(drop=True)
    return symbols['Symbol'].unique()

def get_symbol_changes():
    df = pd.read_csv(CONFIG_DIR + f'/{MODULE}/symbolchange.csv')
    df['Date of Change'] = pd.to_datetime(df['Date of Change'])
    df['Date of Change'] = pd.to_datetime(df['Date of Change'], format="%Y-%m-%d")
    df = df.loc[df['Date of Change'] >= CUTOFF_DATE].reset_index(drop=True)
    df = df.sort_values(by='Date of Change').reset_index(drop=True)
    return df[['Date of Change', 'Old Symbol', 'New Symbol']]

def get_older_symbols(symbol):
    df = get_symbol_changes()
    df = df.loc[df['New Symbol'] == symbol].reset_index(drop=True)
    return list(df['Old Symbol'].unique())

def get_isin(symbol):
    df = pd.read_csv(CONFIG_DIR + f'/{MODULE}/EQUITY_L.csv')
    df = df.loc[df['Symbol'] == symbol].reset_index(drop=True)
    assert df.shape[0] == 1
    return df['ISIN'].values[0]

# ----------------------------------------------------------------------------------------------
if __name__ == '__main__':
    print('Testing nse_config ... ', end='')

    assert len(get_symbols(['ind_nifty50list'])) == 50
    assert len(get_symbols(['ind_nifty100list'])) == 100
    assert len(get_symbols(['ind_nifty50list', 'ind_nifty100list'])) == 100

    sc_df = get_symbol_changes()
    sc_df = sc_df.loc[sc_df['Old Symbol'].isin(['CADILAHC', 'WABCOINDIA'])]
    assert [d.astype(str)[0:10] for d in sc_df['Date of Change'].values] == \
           ['2022-03-07', '2022-04-01']

    assert get_older_symbols('ZYDUSLIFE') == ['CADILAHC']
    assert get_older_symbols('ZFCVINDIA') == ['WABCOINDIA']

    assert get_isin('ZYDUSLIFE') == 'INE010B01027'
    print('All OK')