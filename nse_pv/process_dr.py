"""
NSE process daily market reports
Usage: [year]
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import datetime
import os
import sys
import glob
from io import BytesIO
from zipfile import ZipFile
import pandas as pd
from pygeneric.datetime_utils import elapsed_time
import fin_data.common.nse_symbols as nse_symbols
from pygeneric.archiver import Archiver

PATH_1 = os.path.join(DATA_ROOT, '01_nse_pv/02_dr')
PATH_2 = os.path.join(DATA_ROOT, '01_nse_pv/02_dr/processed')

''' --------------------------------------------------------------------------------------- '''
def remove_existing_files(file_regex, verbose=False):
    elapsed_time(1)

    files_to_remove = glob.glob(os.path.join(PATH_2, file_regex))

    if len(files_to_remove) > 0:
        if verbose: print(f'Removing {len(files_to_remove)} files', end='. ')
        [os.remove(f) for f in files_to_remove]
    else:
        if verbose: print('No files to remove', end='. ')

    if verbose:
        print('time check (remove files):', elapsed_time(1), 'seconds', end='. ')
    return

''' --------------------------------------------------------------------------------------- '''
def process_index_reports(year, verbose=False):
    elapsed_time(0)

    archive_files = glob.glob(os.path.join(PATH_1, f'{year}/**/indices_close.zip'))
    if verbose:
        print(len(archive_files), 'archive files:')
        print(archive_files)

    def read_index_file(bytes, index_file_name):
        df_x = pd.read_csv(bytes)
        dtstr = index_file_name.split('.')[0][14:]
        df_x['Index Date'] = '%s-%s-%s' % (dtstr[4:], dtstr[2:4], dtstr[0:2])
        return df_x

    archives = [Archiver(f, mode='r', compression='zip') for f in archive_files]
    df = pd.concat([read_index_file(BytesIO(a.get(f)), f) for a in archives for f in a.keys()], axis=0)
    df = df[[col for col in df.columns if 'Unnamed' not in col]]
    df['Prev Close'] = df['Closing Index Value'] - df['Points Change']
    df.rename(columns={'Index Date': 'Date',
                       'Open Index Value': 'Open', 'High Index Value': 'High',
                       'Low Index Value': 'Low', 'Closing Index Value': 'Close'}, inplace=True)
    df = df[['Date', 'Index Name', 'Open', 'High', 'Low', 'Close',
             'Prev Close', 'Volume', 'Turnover (Rs. Cr.)', 'P/E', 'P/B', 'Div Yield']]
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values(by=['Index Name', 'Date'], inplace=True)
    df.reset_index(drop=True, inplace=True)

    df.to_parquet(os.path.join(PATH_2, f'{year}/index_bhavcopy_all.csv.parquet'),
                  index=False, engine='pyarrow', compression='gzip')

    if verbose:
        print(df.columns)
        df.to_csv(os.path.join(LOG_DIR, 'df_index.csv'))

    print('Index bhavcopy: from %s to %s, %d days' %
          (df['Date'].values[0].astype('datetime64[D]'),
           df['Date'].values[-1].astype('datetime64[D]'),
           len(df['Date'].unique())))

    print('time check (total time taken):', elapsed_time(0), 'seconds')

    return

''' --------------------------------------------------------------------------------------- '''
def process_etf_reports(year, verbose=False):
    elapsed_time(0)

    archive_files = glob.glob(os.path.join(PATH_1, f'{year}/**/PR.zip'))
    if verbose:
        print(len(archive_files), 'archive files:')
        print(archive_files)

    def read_pr_files(archive_file, verbose=False):
        archive = Archiver(archive_file, mode='r', compression='zip')
        if verbose:
            etf_files = [f1 for f in archive.keys()
                         for f1 in ZipFile(BytesIO(archive.get(f))).namelist()
                         if 'etf' in f1]
            print(f'{archive_file}: ', etf_files)

        def read_etf(bytes, etf_file_name):
            if verbose:
                print(f'Reading {etf_file_name}, ', end='')
            df_x = pd.read_csv(bytes, encoding='cp1252')
            dtstr = etf_file_name.split('.')[0][3:]
            df_x['Date'] = f'20%s-%s-%s' % (dtstr[4:], dtstr[2:4], dtstr[0:2])
            if verbose:
                print('shape:', df_x.shape)
            return df_x
        dfs = pd.concat([read_etf(BytesIO(ZipFile(BytesIO(archive.get(f))).read(f1)), f1)
                         for f in archive.keys()
                         for f1 in ZipFile(BytesIO(archive.get(f))).namelist()
                         if 'etf' in f1
                         ])
        return dfs

    df = pd.concat([read_pr_files(f, verbose) for f in archive_files])

    df.rename(columns={'SERIES': 'Series', 'SYMBOL': 'Symbol',
                       'OPEN PRICE': 'Open', 'HIGH PRICE': 'High',
                       'LOW PRICE': 'Low', 'CLOSE PRICE': 'Close',
                       'PREVIOUS CLOSE PRICE': 'Prev Close',
                       'NET TRADED QTY': 'Volume'}, inplace=True)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values(by=['Symbol', 'Date']).reset_index(drop=True)

    df.insert(0, 'Date', df.pop('Date'))
    df.insert(1, 'Symbol', df.pop('Symbol'))
    df.insert(2, 'UNDERLYING', df.pop('UNDERLYING'))
    df.insert(3, 'SECURITY', df.pop('SECURITY'))
    df.insert(4, 'Series', df.pop('Series'))

    df.to_parquet(os.path.join(PATH_2, f'{year}/etf_bhavcopy_all.csv.parquet'),
                  index=False, engine='pyarrow', compression='gzip')

    if verbose:
        print(df.shape, df.columns)
        df.to_csv(os.path.join(LOG_DIR, 'df_etf.csv'))

    print('ETF bhavcopy: from %s to %s, %d days' %
          (df['Date'].values[0].astype('datetime64[D]'), df['Date'].values[-1].astype('datetime64[D]'),
           len(df['Date'].unique())))

    print('time check (total time taken):', elapsed_time(0), 'seconds')

    return

''' --------------------------------------------------------------------------------------- '''
def process_cm_reports(year, symbols=None, verbose=False):
    elapsed_time([0, 1])

    archive_files = glob.glob(os.path.join(PATH_1, f'{year}/**/cm_bhavcopy.zip'))
    if verbose:
        print(len(archive_files), 'cm_bhavcopy files:')
        print(archive_files)

    def read_cm_bhavcopy_files(archive_file, verbose=False):
        archive = Archiver(archive_file, mode='r', compression='zip')
        if verbose:
            print(f'{archive_file} cm_bhavcopy files: ', archive.keys())
        df_x = pd.concat([pd.read_csv(BytesIO(archive.get(f)), compression={'method':'zip'})
                          for f in archive.keys()])
        return df_x

    df = pd.concat([read_cm_bhavcopy_files(f, verbose) for f in archive_files], axis=0)
    df = df[[col for col in df.columns if 'Unnamed' not in col]]

    archive_files = glob.glob(os.path.join(PATH_1, f'{year}/**/MTO.zip'))
    if verbose:
        print(len(archive_files), 'MTO files:')
        print(archive_files)

    def read_mto_files(archive_file, verbose=False):
        archive = Archiver(archive_file, mode='r', compression='zip')
        if verbose:
            print(f'{archive_file} mto files: ', archive.keys())

        def report_date(f):
            dt = f.split('\\')[-1].split('.')[0].split('_')[-1]
            return datetime.date(year=int(dt[4:]), month=int(dt[2:4]), day=int(dt[0:2]))

        cols = ['c1', 'c2', 'Symbol', 'Series', 'Volume_MTO', 'Delivery Volume', 'Delivery Volume %']
        df_x = pd.concat([pd.concat(
            [pd.read_csv(BytesIO(archive.get(f)), skiprows=3, names=cols, header=0),
                                   pd.DataFrame({'Date': [report_date(f)]})],
                                  axis=1) for f in archive.keys()], axis=0)
        df_x.fillna(method='ffill', inplace=True)
        return df_x

    df2 = pd.concat([read_mto_files(f, verbose) for f in archive_files], axis=0)
    df2 = df2[[col for col in df2.columns if 'Unnamed' not in col]]

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
                  on=['Date', 'Symbol', 'Series'], how='left').reset_index(drop=True)
    if verbose:
        print('time check (load files):', elapsed_time(1), 'seconds')

    all_symbols = sorted(df['Symbol'].unique())
    print(len(all_symbols), 'symbols found in raw data')
    symbols_to_process = all_symbols if symbols is None else symbols
    print(f'Processing {len(symbols_to_process)} symbols :::')

    ''' i think this is fishy - closely check in future '''
    sc_df = nse_symbols.get_symbol_changes()
    for idx, row in sc_df.iterrows():
        df.loc[df['Symbol'] == row['Old Symbol'], 'Symbol'] = row['New Symbol']
        symbols_to_process.append(row['New Symbol'])
        if row['Old Symbol'] in symbols_to_process:
            symbols_to_process.remove(row['Old Symbol'])

    if verbose:
        print('time check (symbol changes):', elapsed_time(1), 'seconds')

    df = df.loc[df['Symbol'].isin(symbols_to_process)]
    df['Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d")
    df = df.sort_values(by=['Symbol', 'Date']).reset_index(drop=False)
    if verbose:
        print(f'Done. {len(symbols_to_process)} processed. Data shape:', df.shape)
        print('time check (filtering):', elapsed_time(1), 'seconds')

    df = df[['Date', 'Symbol', 'Series', 'Open', 'High', 'Low', 'Close', 'Prev Close',
             'Volume', 'Volume_MTO', 'Traded Value', 'No Of Trades',
             'Delivery Volume', 'Delivery Volume %']]
    df.to_parquet(os.path.join(PATH_2, f'{year}/cm_bhavcopy_all.csv.parquet'),
                  index=False, engine='pyarrow', compression='gzip')

    dates_range = sorted(df['Date'].unique())
    first_date  = dates_range[0].astype('datetime64[D]')
    last_date   = dates_range[-1].astype('datetime64[D]')
    date_1yago  = datetime.datetime(last_date.astype(object).year - 1,
                                    last_date.astype(object).month,
                                    last_date.astype(object).day)
    print('Year %d: %d symbols, %d days, first date: %s, last_date: %s, date_1yago: %s'
          % (year, len(df['Symbol'].unique()), len(df['Date'].unique()), first_date, last_date,
             date_1yago.strftime('%Y-%m-%d')))

    print('time check (total time taken):', elapsed_time(0), 'seconds')

    return

''' --------------------------------------------------------------------------------------- '''
# FO processing: TO DO
"""def read_raw_files(path_regex, n_days=0, dr_type=None, verbose=False):
    csv_files = glob.glob(path_regex)

    n_files = n_days if 0 < n_days < len(csv_files) else len(csv_files)
    if verbose:
        print(f'Loading {n_files} / {len(csv_files)} raw files ... ', end='')

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

    return df"""

"""def read_archive(path_regex, dr_type=None, verbose=False):
    archive_files = glob.glob(path_regex)
    if verbose:
        print('archive_files:')
        print(archive_files)
        print(f'\nLoading {len(archive_files)} archives ... ', end='')
    archives = [Archiver(f, mode='r', compression='zip') for f in archive_files]

    if dr_type is None:
        df = pd.concat([pd.read_csv(BytesIO(a.get(f))) for a in archives for f in a.keys()], axis=0)
    elif dr_type == 'MTO':
        def report_date(f):
            dt = f.split('\\')[-1].split('.')[0].split('_')[-1]
            return datetime.date(year=int(dt[4:]), month=int(dt[2:4]), day=int(dt[0:2]))

        cols = ['c1', 'c2', 'Symbol','Series', 'Volume_MTO', 'Delivery Volume', 'Delivery Volume %']
        '''df = pd.concat([pd.concat([pd.read_csv(f, skiprows=3, names=cols, header=0),
                                   pd.DataFrame({'Date': [report_date(f)]})],
                                  axis=1) for f in csv_files[-n_files:]], axis=0)
        df.fillna(method='ffill', inplace=True)'''
        df = pd.DataFrame()
    else:
        df = pd.DataFrame()

    df = df[[col for col in df.columns if 'Unnamed' not in col]]
    if verbose:
        print('Done, shape:', df.shape)

    return df"""

"""
def process_fao_bhavcopy(year, symbols=None, n_days=0, verbose=False):
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
"""

def wrapper(year, verbose=False):
    print(f'\nProcessing daily reports for year {year}...')
    os.makedirs(os.path.join(PATH_2, f'{year}'), exist_ok=True)

    print('Removing existing files: ', end='')
    remove_existing_files(f'{year}/*_bhavcopy*.csv*', verbose=verbose)
    print('Done\n')

    print('Processing Index Daily Reports ... Start')
    process_index_reports(year, verbose=verbose)
    print('Processing Index Daily Reports ... Done\n')

    print('Processing ETF Daily Reports ... Start')
    process_etf_reports(year, verbose=verbose)
    print('Processing ETF Daily Reports ... Done\n')

    print('Processing CM Daily Reports ... Start')
    symbols = None  # tst_syms
    process_cm_reports(year, symbols=symbols, verbose=verbose)
    print('Processing CM Daily Reports ... Done\n')

    """
    print('FAO processing ... Start')
    # symbols = tst_syms + ['NIFTY', 'BANKNIFTY']
    symbols = list(api.nse_symbols.get_symbols(['ind_nifty100list'])) + ['NIFTY', 'BANKNIFTY']
    process_fao_bhavcopy(year, symbols=symbols, n_days=n_days, verbose=verbose)
    print('FAO processing ... Done')
    """

    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    tst_syms = ['ASIANPAINT', 'BRITANNIA', 'HDFC', 'ICICIBANK', 'IRCTC', 'JUBLFOOD', 'ZYDUSLIFE']
    verbose = False
    year = datetime.date.today().year if len(sys.argv) == 1 else int(sys.argv[1])
    wrapper(year, verbose=verbose)

