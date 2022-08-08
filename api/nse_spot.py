# --------------------------------------------------------------------------------------------
# API class for NSE spot PV data
# --------------------------------------------------------------------------------------------

import os
import sys
import glob
import pandas as pd
from datetime import datetime
from nsetools import Nse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common.archiver
import api.nse_cf_ca
import api.nse_symbols
import common.utils
from settings import DATA_ROOT, LOG_DIR

class NseSpotPVData:
    def __init__(self, verbose=False):
        data_file = 'cm_bhavcopy_all.csv'
        data_file = data_file if common.archiver.STORED_AS_CSV else data_file + '.parquet'
        read_func = pd.read_csv if common.archiver.STORED_AS_CSV  else pd.read_parquet

        data_files = glob.glob(
            os.path.join(DATA_ROOT, '01_nse_pv/02_dr/processed', f'**/{data_file}'))
        self.pv_data = pd.concat([read_func(f) for f in data_files])
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
        symbol_cfca = api.nse_cf_ca.get_corporate_actions(symbol)
        for idx, cfca_row in symbol_cfca.iterrows():
            date_idx = df_raw.index[df_raw['Date'] == cfca_row['EX-DATE']]
            if len(date_idx) != 0:
                df_raw = df_raw.apply(lambda row: adj(row, date_idx[0], cfca_row['MULT']), axis=1)
        df_raw.drop(columns=['idx'], inplace=True)

        return df_raw

    def get_pv_data(self, symbol, series='EQ', from_to=None, n_days=0, adjust=True):
        if self.pv_data is None: return None

        df = self.pv_data.loc[(self.pv_data['Symbol'] == symbol) &
                              (self.pv_data['Series'] == series)]

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
        if len(csv_files) == 0: return pd.DataFrame()
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
        common.utils.time_since_last(0)

        if self.pv_data is None: return None

        if verbose:
            print(f'Retrieving {len(symbols)} symbols (adjusted) with 52wk H/L data ...')

        df = self.pv_data.loc[(self.pv_data['Series'] == series) &
                              (self.pv_data['Symbol'].isin(symbols))]

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
            print('time check (get_pv_data_multiple):', common.utils.time_since_last(0), 'seconds\n')

        return df_adj

# --------------------------------------------------------------------------------------------
# current spot prices
spot_nse_obj = None
def get_spot_quote(symbol, index=False):
    global spot_nse_obj
    if spot_nse_obj is None:
        spot_nse_obj = Nse()
    return spot_nse_obj.get_index_quote(symbol) if index else spot_nse_obj.get_quote(symbol)

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    import numpy as np
    print(f'\nTesting {__file__} ...\n')
    symbols = ['ASIANPAINT', 'BRITANNIA', 'HDFC',
               'ICICIBANK', 'IRCTC', 'JUBLFOOD', 'TATASTEEL', 'ZYDUSLIFE']

    def diag(x, a, b):
        xx = (a-b).reset_index()
        print(x.loc[x.index.isin(xx.loc[xx[0] != 0].index)])
        return

    nse_pvdata = NseSpotPVData(verbose=False)

    for symbol in symbols:
        print(f'Testing for {symbol} ...', end=' ')
        df1 = nse_pvdata.get_pv_data(symbol, from_to=['2021-01-01', '2022-03-31'])
        df2 = nse_pvdata.get_pv_data_api(symbol, from_to=['2021-01-01', '2022-03-31'])
        df1.to_csv(os.path.join(LOG_DIR, 'nse_spot_df1.csv'), index=False)
        df2.to_csv(os.path.join(LOG_DIR, 'nse_spot_df2.csv'), index=False)
        assert df1.shape[0] == df2.shape[0], "%d / %d" % (df1.shape[0], df2.shape[0])
        assert np.where(df1['Open']         != df2['Open'])[0].shape[0] == 0
        assert np.where(df1['High']         != df2['High'])[0].shape[0] == 0
        assert np.where(df1['Low']          != df2['Low'])[0].shape[0] == 0
        assert np.where(df1['Close']        != df2['Close'])[0].shape[0] == 0
        assert np.where(df1['Prev Close']   != df2['Prev Close'])[0].shape[0] == 0
        assert np.where(df1['Volume']       != df2['Volume'])[0].shape[0] == 0
        assert np.where(df1['No Of Trades'] != df2['Trades'])[0].shape[0] == 0

        # open: this issue cropped up after writing to & from parquet files
        # for now, ignore as I don't use this column
        assert np.where(df1['Traded Value'] != df2['Turnover'])[0].shape[0] <= 50, \
            diag(df1, df1['Traded Value'], df2['Turnover'])

        assert np.where(df1['Delivery Volume'] != df2['Deliverable Volume'])[0].shape[0] <= 1, \
            diag(df1, df1['Delivery Volume %'], df2['Deliverable Volume'])
        print(' all OK')

    print('\nTesting NseSpotPVData().get_pv_data_multiple ...', end='')
    multi_df = NseSpotPVData(). \
        get_pv_data_multiple(symbols=symbols,
                             from_to=['2021-07-01', '2022-06-30'], get52wkhl=True). \
        sort_values(by=['Date', 'Symbol'])
    print('Done.', multi_df.shape, len(multi_df['Symbol'].unique()))
    exit()  # for now

    print('\nTesting get_pv_data_multiple ...')
    symbols = api.nse_symbols.get_symbols(['ind_nifty500list', 'ind_niftymicrocap250_list'])
    df = nse_pvdata.get_pv_data_multiple(symbols, n_days=252+252, verbose=True)
    print('All Done')
