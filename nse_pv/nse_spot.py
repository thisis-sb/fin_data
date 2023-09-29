"""
API class & other methods for NSE spot PV data
"""
''' ------------------------------------------------------------------------------------------ '''

from fin_data.env import *
import os
import sys
import glob
import pandas as pd
from numpy import datetime_as_string
from datetime import datetime, date, timedelta
import fin_data.common.nse_cf_ca as nse_cf_ca
import fin_data.common.nse_symbols as nse_symbols
import pygeneric.http_utils as http_utils

''' ------------------------------------------------------------------------------------------ '''
class NseSpotPVData:
    def __init__(self, verbose=False):
        self.data_path = os.path.join(DATA_ROOT, '01_nse_pv/02_dr')

        data_files = glob.glob(os.path.join(self.data_path, 'processed/**/cm_bhavcopy_all.csv.parquet'))
        self.pv_data = pd.concat([pd.read_parquet(f) for f in data_files])
        self.pv_data.sort_values(by='Date', inplace=True)
        self.pv_data.reset_index(drop=True, inplace=True)
        if verbose:
            print('pv_data.shape shape:', self.pv_data.shape, end=', ')
            print('%d symbols, %d market days' % (len(self.pv_data['Symbol'].unique()),
                                                  len(self.pv_data['Date'].unique())))

        data_files = glob.glob(os.path.join(self.data_path,
                                            'processed/**/index_bhavcopy_all.csv.parquet'))
        md_idx = pd.read_excel(os.path.join(CONFIG_ROOT, '01_fin_data.xlsx'), sheet_name='indices')
        self.pv_data_index = pd.concat([pd.read_parquet(f) for f in data_files])
        self.pv_data_index = pd.merge(self.pv_data_index, md_idx, on='Index Name', how='left')
        self.pv_data_index.sort_values(by=['Symbol', 'Date'], inplace=True)
        self.pv_data_index.reset_index(drop=True, inplace=True)
        if verbose:
            print('pv_data_index shape:', self.pv_data_index.shape, end=', ')
            print('%d indices, mapped to %d symbols, %d market days' %
                  (len(self.pv_data_index['Index Name'].unique()),
                   len(self.pv_data_index['Symbol'].unique()),
                   len(self.pv_data_index['Date'].unique())))

        data_files = glob.glob(os.path.join(self.data_path,
                                            'processed/**/etf_bhavcopy_all.csv.parquet'))
        md_etf = pd.read_excel(os.path.join(CONFIG_ROOT, '01_fin_data.xlsx'), sheet_name='nse_etf')
        self.pv_data_etf = pd.concat([pd.read_parquet(f) for f in data_files])
        self.pv_data_etf = pd.merge(self.pv_data_etf, md_etf,
                                    on=['Symbol', 'SECURITY', 'UNDERLYING'], how='left')
        self.pv_data_etf.sort_values(by=['Symbol', 'Date'], inplace=True)
        self.pv_data_etf.reset_index(drop=True, inplace=True)
        if verbose:
            print('pv_data_etf shape:', self.pv_data_etf.shape, end=', ')
            print('%d etfs symbols, %d market days' %
                  (len(self.pv_data_etf['Symbol'].unique()),
                   len(self.pv_data_etf['Date'].unique())))
            self.pv_data_etf.to_csv(os.path.join(LOG_DIR, 'df.csv'))

        self.nse_ca_obj = nse_cf_ca.NseCorporateActions(verbose=verbose)

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

        symbol_cfca = self.nse_ca_obj.get_cf_ca_multipliers(symbol)
        for idx, cfca_row in symbol_cfca.iterrows():
            for col in cols1:
                df_raw.loc[df_raw['Date'] < cfca_row['Ex Date'], col] =\
                    round(df_raw.loc[df_raw['Date'] < cfca_row['Ex Date'], col] / cfca_row['MULT'], 2)

        return df_raw

    def get_pv_data(self, symbols, series='EQ', from_to=None, get52wkhl=True, verbose=False):
        if type(symbols) == str:
            df = self.pv_data.loc[self.pv_data['Symbol'] == symbols]
        elif type(symbols) == list:
            df = self.pv_data.loc[self.pv_data['Symbol'].isin(symbols)]
        else:
            raise ValueError(f'Invalid argument symbols type {type(symbols)}')

        if series is not None:
            df = df.loc[df['Series'] == series]

        if from_to[1] is None:
            df = df.loc[df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d')]
        else:
            df = df.loc[(df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d')) &
                        (df['Date'] <= datetime.strptime(from_to[1], '%Y-%m-%d'))]

        if type(symbols) == str:
            df = self.adjust_for_corporate_actions(
                symbols, df,
                ['Prev Close', 'Open', 'High', 'Low', 'Close'],
                ['Volume', 'Volume_MTO', 'Traded Value', 'No Of Trades', 'Delivery Volume']
            )
            df.sort_values(by='Date', inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df

        """ To do: Fix performance problems + 52_wk_HL """
        n_adj = 0
        adjusted_dfs = []
        for symbol in df['Symbol'].unique():
            df1 = df.loc[df['Symbol'] == symbol].reset_index(drop=True)
            df1 = self.adjust_for_corporate_actions(
                symbol, df1,
                ['Prev Close', 'Open', 'High', 'Low', 'Close'],
                ['Volume', 'Volume_MTO', 'Traded Value', 'No Of Trades', 'Delivery Volume']
            )
            """if get52wkhl:
                df1 = self.get_52week_high_low(df1)"""
            adjusted_dfs.append(df1)
            n_adj += 1
            if verbose and n_adj % 100 == 0:
                print(f'  {n_adj} adjusted')
        if verbose:
            print(f'Done. {n_adj} adjusted')

        df_adj = pd.concat([x for x in adjusted_dfs])
        df_adj.sort_values(by='Date', inplace=True)
        df_adj.reset_index(drop=True, inplace=True)
        return df_adj

    def get_latest_closing_prices(self, symbols, series='EQ'):
        date_from = (datetime.today() - timedelta(10)).strftime('%Y-%m-%d')
        df = self.get_pv_data(symbols, series=series, from_to=[date_from, None])
        last_date = df['Date'].unique()[-1]
        df = df.loc[df['Date'] == last_date].reset_index(drop=True)
        return df

    def get_avg_closing_price(self, symbol, mid_point, band=5, series='EQ'):
        try:
            date1 = (datetime.strptime(mid_point, '%Y-%m-%d') - timedelta(days=3*band))
            date2 = (datetime.strptime(mid_point, '%Y-%m-%d') + timedelta(days=3*band))
            from_to = [date1.strftime('%Y-%m-%d'), date2.strftime('%Y-%m-%d')]
            pv_df = self.get_pv_data(symbol, series=series, from_to=from_to)
            pv_df = pv_df[['Date', 'Close']]
            if pv_df.shape[0] == 0:
                raise ValueError('No PV data found')

            pv_df['MP'] = mid_point
            pv_df['MP'] = pd.to_datetime(pv_df['MP'])
            pv_df['DD'] = abs(pv_df['MP'] - pv_df['Date'])

            xx = pv_df.sort_values(by='DD')
            actual_mid_point = xx.reset_index(drop=True).loc[0, 'Date']
            mid_point_idx = pv_df.loc[pv_df['Date'] == actual_mid_point].index[0]

            pv_df = pv_df[mid_point_idx - (band - 1):mid_point_idx + (band + 1)]

            return [datetime_as_string(pv_df['Date'].values[0], unit='D'),
                    datetime_as_string(pv_df['Date'].values[-1], unit='D'),
                    round(pv_df['Close'].mean(), 2)]
        except Exception as e:
            raise ValueError('average_share_price: %s %s [%s]' % (symbol, mid_point, e))

    ''' get_index_pv_data -------------------------------------------------------------------- '''
    def get_index_pv_data(self, symbols, from_to):
        if type(symbols) == str:
            df = self.pv_data_index.loc[self.pv_data_index['Symbol'] == symbols]
        elif type(symbols) == list:
            df = self.pv_data_index.loc[self.pv_data_index['Symbol'].isin(symbols)]
        else:
            raise ValueError(f'Invalid argument symbols type {type(symbols)}')

        if from_to[1] is None:
            df = df.loc[df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d')]
        else:
            df = df.loc[(df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d')) &
                        (df['Date'] <= datetime.strptime(from_to[1], '%Y-%m-%d'))]

        ''' messy workaround right now'''
        if from_to[0] < '2018-01-01' and type(symbols) == str:
            legacy_files = glob.glob(os.path.join(self.data_path, 'legacy/%s/*.csv' % symbols))
            legacy_df = pd.concat([pd.read_csv(f) for f in legacy_files])
            legacy_df['Date'] = pd.to_datetime(legacy_df['Date'])
            legacy_df = legacy_df.loc[legacy_df['Date'] >= from_to[0]]  # know that it stops E2017
            legacy_df['Index Name'] = df['Index Name'].values[0]
            legacy_df['Symbol'] = symbols
            legacy_df['Volume'] = legacy_df['Volume'] * 1000  # sure ??
            legacy_df.drop(columns='Adj Close', inplace=True)
            df = pd.concat([df, legacy_df]).sort_values(by='Date')

        df.reset_index(drop=True, inplace=True)
        return df

    ''' get_etf_pv_data -------------------------------------------------------------------- '''
    def get_etf_pv_data(self, symbols, from_to):
        if type(symbols) == str:
            df = self.pv_data_etf.loc[self.pv_data_etf['Symbol'] == symbols]
        elif type(symbols) == list:
            df = self.pv_data_etf.loc[self.pv_data_etf['Symbol'].isin(symbols)]
        else:
            raise ValueError(f'Invalid argument symbols type {type(symbols)}')

        if from_to[1] is None:
            df = df.loc[df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d')]
        else:
            df = df.loc[(df['Date'] >= datetime.strptime(from_to[0], '%Y-%m-%d')) &
                        (df['Date'] <= datetime.strptime(from_to[1], '%Y-%m-%d'))]

        df.reset_index(drop=True, inplace=True)
        return df

''' ------------------------------------------------------------------------------------------ '''
def get_spot_quote(symbols, index=False):
    """
    Only EQ & IDX supported currently.
    For others, to do:
    --> check meta-info & check if activeSeries or debtSeries

    https://www.nseindia.com/api/equity-meta-info?symbol=BRITANNIA
    debtSeries --> https://www.nseindia.com/api/quote-bonds?index=BRITANNIA

    https://www.nseindia.com/api/equity-meta-info?symbol=HDFC
    activeSeries --> https://www.nseindia.com/api/quote-equity?symbol=HDFC&series=W3
    """
    if index:
        url = 'https://www.nseindia.com/api/allIndices'
    else:
        if type(symbols) != str:
            raise ValueError(f'Invalid argument symbols type {type(symbols)}')
        url = 'https://www.nseindia.com/api/quote-equity?symbol=' + symbols

    get_dict = http_utils.HttpDownloads().http_get_json(url)

    if not index:
        return {
            'Symbol': get_dict['info']['symbol'],
            'Series':'EQ',
            'Date': get_dict['metadata']['lastUpdateTime'][0:11],
            'epoch': str(int(datetime.strptime(get_dict['metadata']['lastUpdateTime'], '%d-%b-%Y %H:%M:%S').timestamp())),
            'Open': get_dict['priceInfo']['open'],
            'High': get_dict['priceInfo']['intraDayHighLow']['max'],
            'Low': get_dict['priceInfo']['intraDayHighLow']['min'],
            'Close': get_dict['priceInfo']['close'],
            'previousClose': get_dict['priceInfo']['previousClose'],
            'lastPrice': get_dict['priceInfo']['lastPrice'],
            'pChange': round(get_dict['priceInfo']['pChange'], 2)
        } if get_dict is not None else None
    else:
        df = pd.DataFrame(get_dict['data'])
        df = df.rename(columns={'index': 'Symbol', 'open': 'Open', 'high': 'High', 'low': 'Low',
                                'last': 'lastPrice', 'percentChange': 'pChange'})
        df['Date'] = get_dict['timestamp'][:11]
        df['epoch'] = str(int(datetime.strptime(get_dict['timestamp'], '%d-%b-%Y %H:%M:%S').timestamp()))
        df['Close'] = None
        df['Series'] = 'IDX'
        df = df[['Symbol', 'Series', 'Date', 'epoch', 'Open', 'High', 'Low', 'Close', 'previousClose', 'lastPrice',
                 'pChange']]

        if type(symbols) == str:
            df = df.loc[df['Symbol'] == symbols]
            return(df.to_dict('records')[0])
        elif type(symbols) == list:
            df = df.loc[df['Symbol'].isin(symbols)]
            return (df.to_dict('records'))
        else:
            raise ValueError(f'Invalid argument symbols type {type(symbols)}')

''' ------------------------------------------------------------------------------------------ '''
if __name__ == '__main__':
    print('Spot Quote EQ :::\n%s\n' % get_spot_quote('ASIANPAINT'))
    print('Spot Quote IX :::\n%s\n' % get_spot_quote(['NIFTY 50', 'NIFTY MIDCAP 150'], index=True))
    xx = NseSpotPVData(verbose=True)
    print('Full test code in wrappers/test_nse_spot.py')
    print(xx.get_index_pv_data('NIFTY 50', ['2010-01-01', '2019-12-31']).shape)
    print(xx.get_index_pv_data('NIFTY 50', ['2018-01-01', '2019-12-31']).shape)
