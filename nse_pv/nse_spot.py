"""
API class & other methods for NSE spot PV data
"""
''' --------------------------------------------------------------------------------------- '''

import os
import sys
import glob
import pandas as pd
from datetime import datetime
from nsetools import Nse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common.nse_cf_ca
import common.nse_symbols
import pygeneric.datetime_utils as datetime_utils
import pygeneric.http_utils as http_utils
from settings import DATA_ROOT, CONFIG_DIR, LOG_DIR

SUB_PATH1 = '01_nse_pv'

''' --------------------------------------------------------------------------------------- '''
class NseSpotPVData:
    def __init__(self, verbose=False):
        data_file = 'cm_bhavcopy_all.csv.parquet'

        data_files = glob.glob(os.path.join(DATA_ROOT, '01_nse_pv/02_dr/processed', f'**/{data_file}'))
        self.pv_data = pd.concat([pd.read_parquet(f) for f in data_files])
        self.pv_data['Date'] = pd.to_datetime(self.pv_data['Date'], format="%Y-%m-%d")

        if verbose:
            print(f'{data_file} shape:', self.pv_data.shape, end=', ')
            print('%d symbols, %d market days' % (len(self.pv_data['Symbol'].unique()),
                                                  len(self.pv_data['Date'].unique())))

        return

    def get_52week_high_low(self, df):
        df['1yago'] = df['Date'].apply(lambda x: datetime(x.year - 1, x.month, x.day))

        def min_max(dates):
            df_1y = df.loc[(df['Date'] >= dates[0]) & (df['Date'] <= dates[1])]
            id_min = df_1y['Low'].idxmin()
            id_max = df_1y['High'].idxmax()
            return [df_1y.at[id_min, 'Low'], df_1y.at[id_min, 'Date'],
                    df_1y.at[id_max, 'High'], df_1y.at[id_max, 'Date']]

        df['min_max'] = df.apply(lambda x: min_max([x['1yago'], x['Date']]), axis=1)
        df['52wk_low'] = df['min_max'].apply(lambda x: x[0])
        df['52wk_low_date'] = df['min_max'].apply(lambda x: x[1])
        df['52wk_high'] = df['min_max'].apply(lambda x: x[2])
        df['52wk_high_date'] = df['min_max'].apply(lambda x: x[3])
        df.drop(columns=['min_max'], inplace=True)

        return df

    def adjust_for_corporate_actions(self, symbol, df_raw, cols1, cols2):
        def adj(df_row, date_idx, mult):
            for col in cols1:
                df_row[col] = df_row[col] / mult if df_row['idx'] < date_idx else df_row[col]
            for col in cols2:
                df_row[col] = df_row[col] * mult if df_row['idx'] < date_idx else df_row[col]
            return df_row

        df_raw['idx'] = df_raw.index
        symbol_cfca = common.nse_cf_ca.get_corporate_actions(symbol)
        for idx, cfca_row in symbol_cfca.iterrows():
            date_idx = df_raw.index[df_raw['Date'] == cfca_row['Ex Date']]
            if len(date_idx) != 0:
                df_raw = df_raw.apply(lambda row: adj(row, date_idx[0], cfca_row['MULT']), axis=1)
        df_raw.drop(columns=['idx'], inplace=True)

        return df_raw

    def get_pv_data(self, symbol, series='EQ', from_to=None, n_days=0, adjust=True):
        if self.pv_data is None:
            return None

        df = self.pv_data.loc[self.pv_data['Symbol'] == symbol]
        if series is not None:
            df = df.loc[df['Series'] == series]

        if from_to is not None:
            date1 = datetime.strptime(from_to[0], '%Y-%m-%d')
            date2 = datetime.strptime(from_to[1], '%Y-%m-%d')
            df = df.loc[(df['Date'] >= date1) & (df['Date'] <= date2)].reset_index(drop=True)
        else:
            n_days_act = n_days if n_days > 0 else df.shape[0]
            df = df.tail(n_days_act).reset_index(drop=True)

        if adjust:
            df = self.adjust_for_corporate_actions(
                symbol, df,
                ['Prev Close', 'Open', 'High', 'Low', 'Close'],
                ['Volume', 'Volume_MTO', 'Traded Value', 'No Of Trades', 'Delivery Volume']
            )

        return df

    def get_pv_data_api(self, symbol, after=None, from_to=None, n_days=0):
        csv_files = glob.glob(os.path.join(DATA_ROOT, '01_nse_pv/01_api', f'{symbol}/*.csv'))
        if len(csv_files) == 0:
            return pd.DataFrame()
        df = pd.concat([pd.read_csv(f) for f in csv_files], axis=0)
        df['Date'] = df['Date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
        df.drop_duplicates(inplace=True)
        df = df.sort_values(by='Date').reset_index(drop=True)

        if after is not None:
            df = df.loc[df['Date'] >= datetime.strptime(after, '%Y-%m-%d')]
        elif from_to is not None:
            date1 = datetime.strptime(from_to[0], '%Y-%m-%d')
            date2 = datetime.strptime(from_to[1], '%Y-%m-%d')
            df = df.loc[(df.Date >= date1) & (df.Date <= date2)].reset_index(drop=True)
        elif n_days != 0:
            df = df.tail(n_days).reset_index(drop=True)

        df = self.adjust_for_corporate_actions(
            symbol, df,
            ['Prev Close', 'Open', 'High', 'Low', 'Close', 'VWAP'],
            ['Volume', 'Turnover', 'Trades', 'Deliverable Volume']
        )

        return df

    def get_pv_data_multiple(self, symbols, series='EQ', after=None, from_to=None, n_days=0,
                             get52wkhl=True, verbose=False):
        datetime_utils.time_since_last(0)

        if self.pv_data is None:
            return None

        if verbose:
            print(f'Retrieving {len(symbols)} symbols (adjusted) with 52wk H/L data ...')

        df = self.pv_data.loc[self.pv_data['Symbol'].isin(symbols)]
        if series is not None:
            df = df.loc[df['Series'] == series]

        df_adj = None
        n_adj = 0
        for symbol in df['Symbol'].unique():
            df1 = df.loc[df['Symbol'] == symbol].reset_index(drop=True)

            if after is not None:
                df1 = df1.loc[df1['Date'] >= datetime.strptime(after, '%Y-%m-%d')]
            elif from_to is not None:
                date1 = datetime.strptime(from_to[0], '%Y-%m-%d')
                date2 = datetime.strptime(from_to[1], '%Y-%m-%d')
                df1 = df1.loc[(df1['Date'] >= date1) & (df1['Date'] <= date2)]
            else:
                n_days_act = n_days if n_days > 0 else df1.shape[0]
                df1 = df1.tail(n_days_act)
            df1.reset_index(drop=True, inplace=True)

            df1 = self.adjust_for_corporate_actions(
                symbol, df1,
                ['Prev Close', 'Open', 'High', 'Low', 'Close'],
                ['Volume', 'Volume_MTO', 'Traded Value', 'No Of Trades', 'Delivery Volume']
            )

            if get52wkhl:
                df1 = self.get_52week_high_low(df1)

            df_adj = df1 if df_adj is None else pd.concat([df_adj, df1], axis=0)
            n_adj += 1
            if verbose and n_adj % 100 == 0:
                print(f'  {n_adj} adjusted')

        df_adj.reset_index(inplace=True)
        df_adj.reset_index(drop=True, inplace=True)

        if verbose:
            print(f'Done. {n_adj} adjusted')
            print('time check (get_pv_data_multiple):', datetime_utils.time_since_last(0), 'seconds\n')

        return df_adj

''' --------------------------------------------------------------------------------------- '''
def get_index_pv(symbol, from_to=None):
    md_idx = pd.read_excel(os.path.join(CONFIG_DIR, '00_meta_data.xlsx'), sheet_name='nse_indices')
    data_files = glob.glob(
        os.path.join(DATA_ROOT, '01_nse_pv/02_dr/processed/**/index_bhavcopy_all.csv.parquet'))
    df = pd.concat([pd.read_parquet(f) for f in data_files])
    df = pd.merge(df, md_idx, on='Index Name', how='left')

    if from_to is not None:
        if from_to[1] is None:
            df = df.loc[(df['Symbol'] == symbol) &
                        (df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d'))]
        else:
            df = df.loc[(df['Symbol'] == symbol) &
                        (df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d')) &
                        (df['Date'] <= datetime.strptime(from_to[1], '%Y-%m-%d'))]
    df.reset_index(drop=True, inplace=True)
    return df

''' --------------------------------------------------------------------------------------- '''
def get_etf_pv(symbol=None, underlying=None, from_to=None):  # later on, series ETF/IDX/EQ
    md_etf = pd.read_excel(os.path.join(CONFIG_DIR, '00_meta_data.xlsx'), sheet_name='nse_etf')
    data_files = glob.glob(
        os.path.join(DATA_ROOT, '01_nse_pv/02_dr/processed/**/etf_bhavcopy_all.csv.parquet'))
    df = pd.concat([pd.read_parquet(f) for f in data_files])
    df = pd.merge(df, md_etf, on=['Symbol', 'SECURITY', 'UNDERLYING'], how='left')
    # print(df.columns)

    if from_to is not None:
        if from_to[1] is None:
            df = df.loc[(df['Underlying Symbol'] == underlying) &
                        (df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d'))]
        else:
            df = df.loc[(df['Underlying Symbol'] == underlying) &
                        (df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d')) &
                        (df['Date'] <= datetime.strptime(from_to[1], '%Y-%m-%d'))]
    df.reset_index(drop=True, inplace=True)
    return df

''' --------------------------------------------------------------------------------------- '''
# current spot prices
spot_nse_obj = None
def get_spot_quote(symbol, index=False):
    if not index:
        url = 'https://www.nseindia.com/api/quote-equity?symbol=' + symbol
        get_dict = http_utils.http_get(url)
        # print(get_dict)
        return {
            'symbol':get_dict['info']['symbol'],
            'Date': get_dict['metadata']['lastUpdateTime'][0:11],
            'Open': get_dict['priceInfo']['open'],
            'High': get_dict['priceInfo']['intraDayHighLow']['max'],
            'Low': get_dict['priceInfo']['intraDayHighLow']['min'],
            'Close': get_dict['priceInfo']['close'],
            'previousClose': get_dict['priceInfo']['previousClose'],
            'lastPrice': get_dict['priceInfo']['lastPrice'],
            'pChange': round(get_dict['priceInfo']['pChange'], 2)
        } if get_dict is not None else None
    else:  # index
        global spot_nse_obj
        if spot_nse_obj is None:
            spot_nse_obj = Nse()
        try:
            get_dict = spot_nse_obj.get_index_quote(symbol)
        except:
            get_dict = None

        # print(get_dict)
        return {
            'name': get_dict['name'],
            'Date': None,
            'Open': None,
            'High': None,
            'Low': None,
            'Close': None,
            'previousClose': None,
            'lastPrice': get_dict['lastPrice'],
            'pChange': round(float(get_dict['pChange']), 2)
        } if get_dict is not None else None

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    import numpy as np

    print(f'\nTesting basic nse_spot ...\n')
    symbols = ['ASIANPAINT', 'BRITANNIA', 'HDFC', 'ICICIBANK', 'IRCTC',
               'JUBLFOOD', 'TATASTEEL', 'ZYDUSLIFE']

    nse_pvdata = NseSpotPVData(verbose=False)

    def diagnostic(x1, x2, c1, c2):
        x = pd.DataFrame({'Date1':x1['Date'], 'Date2':x2['Date'], c1:x1[c1], c2:x2[c2]})
        x.reset_index(drop=True, inplace=True)
        x.to_csv(os.path.join(LOG_DIR, SUB_PATH1, 'df_diagnostic.csv'), index=False)
        return f'{c1}/{c2} {x.loc[x[c1] != x[c2]].shape[0]} rows mismatch'

    def check_data(dates):
        print('Checking for dates', dates, '...')
        for symbol in symbols:
            print(f'  {symbol} ...', end=' ')
            df1 = nse_pvdata.get_pv_data(symbol, series='EQ', from_to=dates)
            df2 = nse_pvdata.get_pv_data_api(symbol, from_to=dates)
            df1.to_csv(os.path.join(LOG_DIR, SUB_PATH1, 'nse_spot_df1.csv'), index=False)
            df2.to_csv(os.path.join(LOG_DIR, SUB_PATH1, 'nse_spot_df2.csv'), index=False)
            assert df1.shape[0] == df2.shape[0], "%d / %d" % (df1.shape[0], df2.shape[0])
            assert np.where(df1['Open'] != df2['Open'])[0].shape[0] == 0
            assert np.where(df1['High'] != df2['High'])[0].shape[0] == 0
            assert np.where(df1['Low'] != df2['Low'])[0].shape[0] == 0
            assert np.where(df1['Close'] != df2['Close'])[0].shape[0] == 0
            assert np.where(df1['Prev Close'] != df2['Prev Close'])[0].shape[0] == 0
            assert np.where(df1['Volume'] != df2['Volume'])[0].shape[0] == 0
            assert np.where(df1['No Of Trades'] != df2['Trades'])[0].shape[0] == 0

            if np.where(df1['Delivery Volume'] != df2['Deliverable Volume'])[0].shape[0] > 6:
                print(diagnostic(df1, df2, 'Delivery Volume', 'Deliverable Volume'), end=' ... ')

            '''open: this issue cropped up after writing to & from parquet files
            for now, ignore as I don't use this column'''
            if np.where(df1['Traded Value'] != df2['Turnover'])[0].shape[0] > 100:
                print(diagnostic(df1, df2, 'Traded Value', 'Turnover'), end=' ... ')

            print('OK')
        print()


    check_data(['2021-01-01', '2022-03-31'])
    check_data(['2022-01-01', '2022-11-30'])
    check_data(['2019-01-01', '2022-03-31'])

    print('\nTesting NseSpotPVData().get_pv_data_multiple ...', end='')
    multi_df = NseSpotPVData(). \
        get_pv_data_multiple(symbols=symbols,
                             from_to=['2021-07-01', '2022-06-30'], get52wkhl=True). \
        sort_values(by=['Date', 'Symbol'])
    print('Done.', multi_df.shape, len(multi_df['Symbol'].unique()))

    print('\nTesting get_index_or_etf_pv ...')
    print(get_index_pv('NIFTY 50', from_to=['2022-09-20', '2022-09-26'])[['Date', 'Symbol', 'Close']])
    print(get_etf_pv(underlying='NIFTY 50', from_to=['2022-09-20', '2022-09-26'])['UNDERLYING'].unique())
    print('Done')

    print('\nTesting get_spot_quote ...')
    print(get_spot_quote('HINDALCO'))
    print(get_spot_quote('ICICITECH'))
    print(get_spot_quote('NIFTY 50', index=True))
    print('Done')

    print('\nTesting for partly paid symbol ...')
    df_pp = nse_pvdata.get_pv_data('AIRTELPP', series='E1', from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    df_pp = nse_pvdata.get_pv_data_multiple(['BHARTIARTL', 'AIRTELPP'],
                                            from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    df_pp = nse_pvdata.get_pv_data_multiple(['BRITANNIA'],
                                            from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    print('Done')

    ''' need to test this - doesn't work ...'''
    """print('\nTesting get_pv_data_multiple ...')
    symbols = api.nse_symbols.get_symbols(['ind_nifty500list', 'ind_niftymicrocap250_list'])
    df = nse_pvdata.get_pv_data_multiple(symbols, n_days=252+252, verbose=True)
    print('Done')"""

    print('\nAll Done')
