import pandas as pd
from global_env import CONFIG_DIR

def get_symbols(file_list, filter='EQ'):
    symbols = pd.DataFrame()
    for f in file_list:
        df = pd.read_csv(CONFIG_DIR + f'/{f}.csv')
        if 'Group' in df.columns:
            df.rename(columns={'Group':'Series'}, inplace=True)
        symbols = pd.concat([symbols, df[['Series', 'Symbol']]])
    if filter is not None:
        symbols = symbols.loc[symbols.Series == filter].reset_index(drop=True)
    return symbols['Symbol'].unique()

def get_symbol_changes():
    df = pd.read_csv(CONFIG_DIR + '/nse_symbol_changes.csv')
    df.columns = df.columns.str.replace(' ', '')
    df['Date'] = pd.to_datetime(df['SM_APPLICABLE_FROM'])
    df = df.loc[df['Date'] >= '2019-01-01'].reset_index(drop=True)
    df = df.sort_values(by='Date').reset_index(drop=True)
    df.rename(columns={'SM_KEY_SYMBOL':'old', 'SM_NEW_SYMBOL':'new'}, inplace=True)
    return df[['Date', 'old', 'new']]

# ----------------------------------------------------------------------------------------------
if __name__ == '__main__':
    print('Testing nse_config ...')

    print(len(get_symbols(['ind_nifty50list', 'ind_nifty100list'])))

    sc_df = get_symbol_changes()
    print(sc_df.loc[sc_df['old'].isin(['CADILAHC', 'WABCOINDIA'])].to_string(index=False))

    '''for symbol in ['BRITANNIA', 'KBCGLOBAL', 'RADIOCITY', 'IRCTC', 'MOTOGENFIN', 'MARINE']:
        xx = get_corporate_actions(symbol)
        print('%s\t%s' % (symbol, {'EX-DATE': list(xx['EX-DATE']), 'MULT': list(xx['MULT'])}))'''