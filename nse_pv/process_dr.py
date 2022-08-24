# --------------------------------------------------------------------------------------------
# NSE process daily market reports
# Usage: year
# --------------------------------------------------------------------------------------------
import datetime
import os
import sys
import glob
import pandas as pd
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common.archiver
import common.utils
import api.nse_symbols
from settings import DATA_ROOT

SUB_PATH1 = '01_nse_pv/02_dr'
SUB_PATH2 = os.path.join(SUB_PATH1, 'processed')

def remove_existing_files(file_regex, verbose=False):
    common.utils.time_since_last(0); common.utils.time_since_last(1)

    files_to_remove = glob.glob(os.path.join(DATA_ROOT, SUB_PATH2, file_regex))

    if len(files_to_remove) > 0:
        if verbose: print(f'Removing {len(files_to_remove)} files', end='. ')
        [os.remove(f) for f in files_to_remove]
    else:
        if verbose: print('No files to remove', end='. ')

    if verbose:
        print('time check (remove files):', common.utils.time_since_last(1), 'seconds', end='. ')
    return

def read_raw_files(path_regex, n_days=0, dr_type=None, verbose=False):
    csv_files = glob.glob(path_regex)

    n_files = n_days if 0 < n_days < len(csv_files) else len(csv_files)
    if verbose: print(f'Loading {n_files} / {len(csv_files)} raw files ... ', end='')

    if dr_type == 'MTO':
        def report_date(f):
            dt = f.split('\\')[-1].split('.')[0].split('_')[-1]
            return datetime.date(year=int(dt[4:]), month=int(dt[2:4]), day=int(dt[0:2]))

        cols = ['c1', 'c2', 'Symbol','Series', 'Volume_MTO', 'Delivery Volume', 'Delivery Volume %']
        df = pd.concat([pd.concat([pd.read_csv(f, skiprows=3, names=cols, header=0),
                                   pd.DataFrame({'Date': [report_date(f)]})],
                                  axis=1) for f in csv_files[-n_files:]], axis=0)
        df.fillna(method='ffill', inplace=True)
    else:
        df = pd.concat([pd.read_csv(f) for f in csv_files[-n_files:]], axis=0)
    df = df[[col for col in df.columns if 'Unnamed' not in col]]
    if verbose: print('Done, shape:', df.shape)

    return df

def pre_process_fao_bhavcopy(year, symbols=None, n_days=0, verbose=False):
    common.utils.time_since_last(0); common.utils.time_since_last(1)

    df = read_raw_files(os.path.join(DATA_ROOT, SUB_PATH1, f'{year}/**/fo*bhav.csv.zip'),
                        n_days=n_days, verbose=verbose)
    df.rename(columns={
        'INSTRUMENT':'Instrument', 'SYMBOL':'Symbol', 'TIMESTAMP':'Date',
        'EXPIRY_DT':'Expiry Date', 'STRIKE_PR':'Strike Price', 'OPTION_TYP':'Option Type',
        'OPEN':'Open', 'HIGH':'High', 'LOW':'Low', 'CLOSE':'Close',
        'SETTLE_PR':'Settlement Price',
        'CONTRACTS':'Traded Contracts', 'VAL_INLAKH':'Traded Value (lakhs)',
        'OPEN_INT':'Open Interest',
        'CHG_IN_OI':'Change in OI'
    }, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Expiry Date'] = (pd.to_datetime(df['Expiry Date']))
    if verbose: print('time check (load files):', common.utils.time_since_last(1), 'seconds')

    print('Processing symbol changes ...')
    sc_df = api.nse_symbols.get_symbol_changes()
    for symbol in symbols:
        xx = sc_df.loc[sc_df['new'] == symbol]
        if xx.shape[0] > 0:
            print('  symbol: %s to %s' % (xx['old'].values[-1], symbol))
            df.loc[df['Symbol'] == xx['old'].values[-1], 'Symbol'] = symbol
    if verbose: print('time check (symbol changes):', common.utils.time_since_last(1), 'seconds')

    all_symbols = sorted(df['Symbol'].unique())
    print(len(all_symbols), 'symbols found in raw data')

    symbols_to_process = all_symbols if symbols is None else symbols
    print(f'Processing {len(symbols_to_process)} symbols :::')
    cols = ['Date'] + [c for c in df.columns if c != 'Date']

    def find_em(series):
        return pd.Series(np.arange(1,len(series)+1), index=series.index)

    for idx, symbol in enumerate(sorted(symbols_to_process)):
        df_s = df[cols].loc[df['Symbol'] == symbol]
        df_s.sort_values(by=['Date', 'Instrument', 'Option Type', 'Strike Price','Expiry Date'],
                         inplace=True)
        df_s.reset_index(drop=True, inplace=True)
        df_s['EMT'] = df_s.groupby(
            ['Date', 'Instrument', 'Strike Price', 'Option Type'])['Expiry Date'].apply(find_em)
        common.archiver.df_to_file(
            df_s, os.path.join(DATA_ROOT, SUB_PATH2, f'{year}/fo_bhavcopy_{symbol}.csv'))
        if (idx+1) % 50 == 0: print(' ', idx+1, 'symbols processed')

    print(len(symbols_to_process), 'symbols processed. Done')
    if verbose: print('time check (process files):', common.utils.time_since_last(1), 'seconds')
    print('time check (total time taken):', common.utils.time_since_last(0), 'seconds')

    return

def get_52week_high_low(df):
    df['1yago'] = df['Date'].apply(lambda x: datetime.datetime(x.year-1, x.month, x.day))

    def min_max(dates):
        df_1y = df.loc[(df['Date'] >= dates[0]) & (df['Date'] <= dates[1])]
        id_min = df_1y['Low'].idxmin()
        id_max = df_1y['High'].idxmax()
        return [df_1y.at[id_min, 'Low'], df_1y.at[id_min, 'Date'],
                df_1y.at[id_max, 'High'], df_1y.at[id_max, 'Date']]

    df['min_max']        = df.apply(lambda x: min_max([x['1yago'], x['Date']]), axis=1)
    df['52wk_low']       = df['min_max'].apply(lambda x: x[0])
    df['52wk_low_date']  = df['min_max'].apply(lambda x: x[1])
    df['52wk_high']      = df['min_max'].apply(lambda x: x[2])
    df['52wk_high_date'] = df['min_max'].apply(lambda x: x[3])
    df.drop(columns=['min_max'], inplace=True)

    return

def pre_process_cm_bhavcopy(year, symbols, filter=None, n_days=0, verbose=False):
    common.utils.time_since_last(0); common.utils.time_since_last(1)

    df  = read_raw_files(os.path.join(DATA_ROOT, SUB_PATH1, f'{year}/**/cm*bhav.csv.zip'),
                         n_days=n_days, verbose=verbose)
    df2 = read_raw_files(os.path.join(DATA_ROOT, SUB_PATH1, f'{year}/**/MTO_*.DAT'),
                         n_days=n_days, dr_type='MTO', verbose=verbose)

    df.rename(columns={'TIMESTAMP':'Date', 'SYMBOL':'Symbol', 'SERIES':'Series',
                       'OPEN':'Open', 'HIGH':'High', 'LOW':'Low', 'CLOSE':'Close',
                       'LAST':'Last', 'PREVCLOSE':'Prev Close',
                       'TOTTRDQTY':'Volume', 'TOTTRDVAL':'Traded Value',
                       'TOTALTRADES':'No Of Trades'}, inplace=True)

    df['Date'] = pd.to_datetime(df['Date'])
    df2['Date'] = pd.to_datetime(df2['Date'])

    df2['% Delivery Volume'] = round(df2['Delivery Volume'] / 100, 4)
    df['Traded Value'] = df['Traded Value']*100000

    df = df.merge(df2[['Date', 'Symbol','Series', 'Volume_MTO', 'Delivery Volume', 'Delivery Volume %']],
                  on=['Date', 'Symbol', 'Series'], how='inner').reset_index(drop=True)
    if verbose: print('time check (load files):', common.utils.time_since_last(1), 'seconds')

    all_symbols = sorted(df['Symbol'].unique())
    print(len(all_symbols), 'symbols found in raw data')
    symbols_to_process = all_symbols if symbols is None else symbols
    print(f'Processing {len(symbols_to_process)} symbols :::')

    sc_df = api.nse_symbols.get_symbol_changes()
    for symbol in symbols_to_process:
        xx = sc_df.loc[sc_df['new'] == symbol]
        if xx.shape[0] > 0:
            df.loc[df['Symbol'] == xx['old'].values[-1], 'Symbol'] = symbol
    if verbose: print('time check (symbol changes):', common.utils.time_since_last(1), 'seconds')

    if filter is not None: df = df.loc[df['Series'] == filter]
    df = df.loc[df['Symbol'].isin(symbols_to_process)]
    df['Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d")
    df = df.sort_values(by=['Symbol', 'Date']).reset_index(drop=False)
    if verbose: print(f'Done. {len(symbols_to_process)} processed. Data shape:', df.shape)
    if verbose: print('time check (filtering):', common.utils.time_since_last(1), 'seconds')

    df = df[['Date', 'Symbol', 'Series', 'Open', 'High', 'Low', 'Close', 'Prev Close',
             'Volume', 'Volume_MTO', 'Traded Value', 'No Of Trades',
             'Delivery Volume', 'Delivery Volume %']]
    common.archiver.df_to_file(df, os.path.join(DATA_ROOT, SUB_PATH2, f'{year}/cm_bhavcopy_all.csv'))

    dates_range = df['Date'].values
    first_date  = dates_range[0].astype('datetime64[D]')
    last_date   = dates_range[-1].astype('datetime64[D]')
    date_1yago  = datetime.datetime(last_date.astype(object).year - 1,
                                    last_date.astype(object).month,
                                    last_date.astype(object).day)
    print('Year %d: %d symbols, %d days, first date: %s, last_date: %s, date_1yago: %s'
          % (year, len(df['Symbol'].unique()), len(df['Date'].unique()), first_date, last_date,
             date_1yago.strftime('%Y-%m-%d')))

    print('time check (total time taken):', common.utils.time_since_last(0), 'seconds')

    return

if __name__ == '__main__':
    tst_syms = ['ASIANPAINT', 'BRITANNIA', 'HDFC', 'ICICIBANK', 'IRCTC', 'JUBLFOOD', 'ZYDUSLIFE']
    n_days, verbose = 0, True
    year = 2022 if len(sys.argv) == 1 else int(sys.argv[1])
    print(f'\nProcessing daily reports for year {year}...')
    os.makedirs(os.path.join(DATA_ROOT, SUB_PATH1, f'{year}'), exist_ok=True)

    print('Removing existing files: ', end='')
    remove_existing_files(f'{year}/*_bhavcopy*.csv*', verbose=verbose)
    print('Done\n')

    print('FAO pre-processing START ...')
    # symbols = tst_syms + ['NIFTY', 'BANKNIFTY']
    symbols = list(api.nse_symbols.get_symbols(['ind_nifty100list'])) + ['NIFTY', 'BANKNIFTY']
    pre_process_fao_bhavcopy(year, symbols=symbols, n_days=n_days, verbose=verbose)
    print('FAO processing ... Done\n')

    print('CM pre-processing START ...')
    # symbols = tst_syms
    symbols = list(api.nse_symbols.get_symbols(['ind_nifty500list',
                                                'ind_niftymicrocap250_list', 'ind_my_custom_index']))
    pre_process_cm_bhavcopy(year, symbols=symbols, filter='EQ', n_days=n_days, verbose=verbose)
    print('CM processing ... Done')
