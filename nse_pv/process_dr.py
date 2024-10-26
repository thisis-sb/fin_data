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
import numpy as np
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

    def read_index_file(bytes, index_file_name):
        df_x = pd.read_csv(bytes)
        dtstr = index_file_name.split('.')[0][14:]
        df_x['Index Date'] = '%s-%s-%s' % (dtstr[4:], dtstr[2:4], dtstr[0:2])
        return df_x

    idx_closing_files = glob.glob(os.path.join(PATH_1, f'{year}/**/indices_close.zip'))
    print('%d index_closing files' % len(idx_closing_files), end='')
    archives = [Archiver(f, mode='r', compression='zip') for f in idx_closing_files]
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
    print(', df.shape:', df.shape)

    df.to_parquet(os.path.join(PATH_2, f'{year}/index_bhavcopy_all.csv.parquet'),
                  index=False, engine='pyarrow', compression='gzip')
    print('time check (total time taken):', elapsed_time(0), 'seconds')

    days = sorted(df.loc[df['Index Name'] == 'Nifty 50']['Date'].unique().astype('datetime64[D]'))
    print('idx bhavcopy: from %s to %s, %d days' % (days[0], days[-1], len(days)))

    return

''' --------------------------------------------------------------------------------------- '''
def process_cm_reports(year, symbols=None, verbose=False):
    elapsed_time([0, 1])

    common_cols_cm_bhavcopy = [
        'Date', 'Symbol', 'ISIN', 'Series', 'Open', 'High', 'Low', 'Close',
        'Prev Close', 'Volume', 'Traded Value', 'No Of Trades'
    ]

    ''' Step 1: Read bhavcopy files - OLD and new/v02 '''
    def read_cm_bhavcopy_files(archive_file, verbose=False):
        archive = Archiver(archive_file, mode='r', compression='zip')
        if verbose:
            print(f'{archive_file} cm_bhavcopy files: ', archive.keys())
        df_x = pd.concat([pd.read_csv(BytesIO(archive.get(f)), compression={'method':'zip'},
                                      keep_default_na=False, engine='pyarrow')
                          for f in archive.keys()])
        return df_x

    ''' Read OLD bhavcopy files '''
    cm_bhavcopy_files = glob.glob(os.path.join(PATH_1, f'{year}/**/cm_bhavcopy.zip'))
    print('%d OLD bhavcopy files' % len(cm_bhavcopy_files), end='')
    if len(cm_bhavcopy_files) > 0:
        df = pd.concat([read_cm_bhavcopy_files(f, verbose) for f in cm_bhavcopy_files], axis=0)
        df = df[[col for col in df.columns if 'Unnamed' not in col]]
        df.rename(columns={
            'TIMESTAMP': 'Date', 'SYMBOL': 'Symbol', 'SERIES': 'Series',
            'OPEN': 'Open', 'HIGH': 'High', 'LOW': 'Low', 'CLOSE': 'Close',
            'LAST': 'Last', 'PREVCLOSE': 'Prev Close',
            'TOTTRDQTY': 'Volume', 'TOTTRDVAL': 'Traded Value', 'TOTALTRADES': 'No Of Trades'},
            inplace=True
        )
        df = df[common_cols_cm_bhavcopy]
    else:
        df = pd.DataFrame(columns=common_cols_cm_bhavcopy)
    print(', df.shape:', df.shape, end='')

    ''' Read NEW (v02) bhavcopy files '''
    cm_bhavcopy_files_v02 = glob.glob(os.path.join(PATH_1, f'{year}/**/cm_bhavcopy_v02.zip'))
    print(', %d NEW bhavcopy files' % len(cm_bhavcopy_files_v02), end='')
    if len(cm_bhavcopy_files_v02) > 0:
        df_v02 = pd.concat([read_cm_bhavcopy_files(f, verbose) for f in cm_bhavcopy_files_v02], axis=0)
        df_v02.rename(columns={
            'TradDt':'Date', 'TckrSymb':'Symbol', 'SctySrs':'Series',
            'OpnPric':'Open', 'HghPric':'High', 'LwPric':'Low', 'ClsPric':'Close',
            'LastPric':'Last', 'PrvsClsgPric':'Prev Close',
            'TtlTradgVol':'Volume', 'TtlTrfVal':'Traded Value', 'TtlNbOfTxsExctd':'No Of Trades'},
            inplace=True
        )
        df_v02 = df_v02[common_cols_cm_bhavcopy]
    else:
        df_v02 = pd.DataFrame(columns=common_cols_cm_bhavcopy)
    print(', df_v02.shape:', df_v02.shape)

    df = pd.concat([df, df_v02], axis=0).reset_index(drop=True)
    if df.shape[0] == 0:
        print('WARNING! No files (either OLD or NEW/v02 found, returning!')
        return
    df['Traded Value'] = df['Traded Value'] * 100000   # TO BE CHECKED

    print('combined cm_bhavcopy df.shape:', df.shape)

    ''' Read MTO files '''
    def read_mto_files(archive_file, verbose=False):
        archive = Archiver(archive_file, mode='r', compression='zip')
        if verbose:
            print(f'{archive_file} mto files: ', archive.keys())

        def report_date(f):
            dt = f.split('\\')[-1].split('.')[0].split('_')[-1]
            return datetime.date(year=int(dt[4:]), month=int(dt[2:4]), day=int(dt[0:2]))

        cols = ['c1', 'c2', 'Symbol', 'Series', 'Volume_MTO', 'Delivery Volume', 'Delivery Volume %']
        df_x = pd.concat([pd.concat([pd.read_csv(BytesIO(archive.get(f)), skiprows=3, names=cols,
                                                 header=0, keep_default_na=False),
                                     pd.DataFrame({'Date': [report_date(f)]})],
                                    axis=1) for f in archive.keys()], axis=0)
        df_x.fillna(method='ffill', inplace=True)
        return df_x

    mto_files = glob.glob(os.path.join(PATH_1, f'{year}/**/MTO.zip'))
    print('%d MTO files' % len(mto_files), end='')
    if len(mto_files) > 0:
        df_mto = pd.concat([read_mto_files(f, verbose) for f in mto_files], axis=0)
        df_mto = df_mto[[col for col in df_mto.columns if 'Unnamed' not in col]]
        df_mto['Date'] = pd.to_datetime(df_mto['Date'])
        df_mto['% Delivery Volume'] = round(df_mto['Delivery Volume'] / 100, 4)
    else:
        df_mto = pd.DataFrame()
    print(', df_mto.shape:', df_mto.shape)

    df['Date'] = pd.to_datetime(df['Date'])

    # 2024-07-17: removed 'Volume_MTO'
    df = df.merge(
        df_mto[['Date', 'Symbol','Series', 'Delivery Volume', 'Delivery Volume %']],
        on=['Date', 'Symbol', 'Series'], how='left'
    ).reset_index(drop=True)

    print('final merged df.shape:', df.shape)
    print('time check (load files):', elapsed_time(1), 'seconds')

    ''' Processing symbol changes '''
    all_symbols = sorted(df['Symbol'].unique())
    print(len(all_symbols), 'symbols found in raw data')
    symbols_to_process = all_symbols if symbols is None else symbols  # WHY?

    ''' i think this is fishy - closely check in future '''
    sc_df = nse_symbols.get_symbol_changes()
    for idx, row in sc_df.iterrows():
        df.loc[df['Symbol'] == row['Old Symbol'], 'Symbol'] = row['New Symbol']
        symbols_to_process.append(row['New Symbol'])
        if row['Old Symbol'] in symbols_to_process:
            symbols_to_process.remove(row['Old Symbol'])
    print('time check (process symbol changes):', elapsed_time(1), 'seconds')

    df = df.loc[df['Symbol'].isin(symbols_to_process)]
    df['Date'] = pd.to_datetime(df['Date'], format="%Y-%m-%d")
    df = df.sort_values(by=['Symbol', 'Date']).reset_index(drop=False)
    print('time check (filtering):', elapsed_time(1), 'seconds')
    print('final, merged & processed df.shape:', df.shape)

    df.to_parquet(os.path.join(PATH_2, f'{year}/cm_bhavcopy_all.csv.parquet'),
                  index=False, engine='pyarrow', compression='gzip')

    dates_range = sorted(df['Date'].unique())
    first_date  = dates_range[0].astype('datetime64[D]')
    last_date   = dates_range[-1].astype('datetime64[D]')
    date_1yago  = datetime.datetime(last_date.astype(object).year - 1,
                                    last_date.astype(object).month,
                                    last_date.astype(object).day)
    print('year: %d: %d symbols, %d days, from: %s, to: %s, date_1yago: %s'
          % (year, len(df['Symbol'].unique()), len(df['Date'].unique()), first_date, last_date,
             date_1yago.strftime('%Y-%m-%d')))

    print('time check (total time taken):', elapsed_time(0), 'seconds')

    return

''' --------------------------------------------------------------------------------------- '''
# New, still wip (subject to appl use cases)
def process_fo_reports(year, verbose=False):
    elapsed_time([0, 1])

    common_cols_fo_bhavcopy = [
        'date', 'instr_type', 'symbol', 'ISIN', 'expiry_date', 'strike_price', 'option_type',
        'open', 'high', 'low', 'close',
        'settlement_price', 'trading_volume', 'open_interest', 'change_in_oi', 'trading_value'
    ]

    def read_fo_bhavcopy_files(archive_file, verbose=False):
        archive = Archiver(archive_file, mode='r', compression='zip')
        if verbose:
            print(f'{archive_file} fo_bhavcopy files: ', archive.keys())
        df_x = pd.concat([pd.read_csv(BytesIO(archive.get(f)), compression={'method': 'zip'},
                                      keep_default_na=False, engine='pyarrow')
                          for f in archive.keys()])
        return df_x

    ''' Read OLD bhavcopy files '''
    fo_bhavcopy_files = glob.glob(os.path.join(PATH_1, f'{year}/**/fo_bhavcopy.zip'))
    print('%d OLD bhavcopy files' % len(fo_bhavcopy_files), end='')
    if len(fo_bhavcopy_files) > 0:
        df = pd.concat([read_fo_bhavcopy_files(f, verbose) for f in fo_bhavcopy_files], axis=0)
        df.rename(columns={
            'TIMESTAMP':'date', 'INSTRUMENT':'instr_type', 'SYMBOL':'symbol', 'EXPIRY_DT':'expiry_date',
            'STRIKE_PR':'strike_price', 'OPTION_TYP':'option_type',
            'OPEN':'open', 'HIGH':'high', 'LOW':'low', 'CLOSE':'close',
            'SETTLE_PR':'settlement_price',
            'CONTRACTS':'trading_volume', 'OPEN_INT':'open_interest', 'CHG_IN_OI':'change_in_oi',
            'VAL_INLAKH':'trading_value'
            },
        inplace=True)
        df['trading_value'] = df['trading_value'] * 100000  # TO BE CHECKED
        it_dict = {'FUTIDX':'IDF', 'OPTIDX':'IDO', 'FUTSTK':'STF', 'OPTSTK':'STO',
                   'FUTIVX':'IDF'}
        df['instr_type'] = df['instr_type'].apply(lambda x: it_dict[x])
        df['ISIN'] = np.nan
        df = df[common_cols_fo_bhavcopy]
    else:
        df = pd.DataFrame(columns=common_cols_fo_bhavcopy)
    print(', df.shape:', df.shape, end='')

    ''' Read NEW (v02) bhavcopy files '''
    fo_bhavcopy_files_v02 = glob.glob(os.path.join(PATH_1, f'{year}/**/fo_bhavcopy_v02.zip'))
    print(', %d NEW bhavcopy files' % len(fo_bhavcopy_files_v02), end='')
    if len(fo_bhavcopy_files_v02) > 0:
        df_v02 = pd.concat([read_fo_bhavcopy_files(f, verbose) for f in fo_bhavcopy_files_v02], axis=0)
        df_v02.rename(columns={
            'TradDt': 'date', 'FinInstrmTp': 'instr_type', 'TckrSymb': 'symbol', 'XpryDt': 'expiry_date',
            'StrkPric': 'strike_price', 'OptnTp': 'option_type',
            'OpnPric': 'open', 'HghPric': 'high', 'LwPric': 'low', 'ClsPric': 'close',
            'SttlmPric': 'settlement_price',
            'OpnIntrst': 'open_interest', 'ChngInOpnIntrst': 'change_in_oi',
            'TtlTradgVol': 'trading_volume', 'TtlTrfVal': 'trading_value',
            'TtlNbOfTxsExctd':'number_of_trades'
            },
            inplace=True)
        df_v02 = df_v02[common_cols_fo_bhavcopy]
    else:
        df_v02 = pd.DataFrame(columns=common_cols_fo_bhavcopy)
    print(', df_v02.shape:', df_v02.shape)

    df = pd.concat([df, df_v02], axis=0).reset_index(drop=True)
    if df.shape[0] == 0:
        print('WARNING! No files (either OLD or NEW/v02 found, returning!')
        return

    df['date'] = pd.to_datetime(df['date'])
    df['expiry_date'] = pd.to_datetime(df['expiry_date'])
    df['strike_price'] = df['strike_price'].replace('', np.nan).astype(float)
    df.insert(0, 'date', df.pop('date'))

    print('combined fo_bhavcopy df.shape:', df.shape)
    print('time check (load files):', elapsed_time(1), 'seconds')

    # NOTE: symbol changes not applied. Takes a very long time. Probably not relevant for FO

    # pickle was the fastest but space was 5x of CSV. parquet is best of both worlds
    for instr in ['IDF', 'IDO', 'STF', 'STO']:
        elapsed_time(1)
        x = df.loc[df['instr_type'] == instr].sort_values(by=['date', 'symbol', 'expiry_date'])
        x.to_parquet(os.path.join(PATH_2, '%d/fo_bhavcopy_%s.csv.parquet' % (year, instr)),
                     index=False, engine='pyarrow', compression='gzip')
        print('  time check fo_bhavcopy_%s: save: %.2f' % (instr, elapsed_time(1)), 'seconds', end='')
        print(f' (shape: {x.shape})')
        y = pd.read_parquet(os.path.join(PATH_2, '%d/fo_bhavcopy_%s.csv.parquet' % (year, instr)))
        print('  time check fo_bhavcopy_%s: read: %.2f' % (instr, elapsed_time(1)), 'seconds')
        assert x.shape == y.shape

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
"""
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
    print(f'Processing daily reports for year {year}...')
    os.makedirs(os.path.join(PATH_2, f'{year}'), exist_ok=True)

    print('Removing existing files: ', end='')
    remove_existing_files(f'{year}/*_bhavcopy*.csv*', verbose=verbose)
    print('Done\n')

    print('Processing Index Daily Reports ... Start')
    process_index_reports(year, verbose=verbose)
    print('Processing Index Daily Reports ... Done\n')

    print('Processing CM Daily Reports ... Start')
    symbols = None  # tst_syms
    process_cm_reports(year, symbols=symbols, verbose=verbose)
    print('Processing CM Daily Reports ... Done\n')

    print('Processing FO Daily Reports ... Start')
    process_fo_reports(year, verbose=verbose)
    print('Processing FO Daily Reports ... Done\n')

    print('Processing ETF Daily Reports ... Start')
    process_etf_reports(year, verbose=verbose)
    print('Processing ETF Daily Reports ... Done\n')

    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    tst_syms = ['ASIANPAINT', 'BRITANNIA', 'HDFC', 'ICICIBANK', 'IRCTC', 'JUBLFOOD', 'ZYDUSLIFE']
    verbose = False
    year = datetime.date.today().year if len(sys.argv) == 1 else int(sys.argv[1])
    wrapper(year, verbose=verbose)

