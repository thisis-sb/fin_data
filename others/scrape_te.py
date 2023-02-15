"""
Scrape real-time data from tradingeconomics.com
Currently: Commodities
"""
''' --------------------------------------------------------------------------------------- '''

import sys
import os
from datetime import date
import pandas as pd
import pygeneric.http_utils as pyg_http_utils

CONFIG_DIR = os.path.join(os.getenv('CONFIG_ROOT'), '11_te')
OUTPUT_DIR = os.path.join(os.getenv('DATA_ROOT'), '11_te')

''' --------------------------------------------------------------------------------------- '''
def get_hist(symbol, category, year, auth_key_str, verbose=False):
    d1, d2 = f'{year}-01-01', f'{year}-12-31'
    url1 = 'https://markets.tradingeconomics.com/chart?s=%s' % symbol
    url2 = '&d1=%s&d2=%s&interval=1d&securify=new&url=%s' % (d1, d2, category)
    url3 = f'&AUTH={auth_key_str}'
    url4 = '&ohlc=0'
    url = url1 + url2 + url3 + url4
    if verbose: print('url:[%s]' % url)

    http_obj = pyg_http_utils.HttpDownloads(website='te')
    data = http_obj.http_get_json(url)
    if data is None:
        return None
    elif 'd1' not in data.keys() or 'd2' not in data.keys() or 'agr' not in data.keys():
        raise ValueError('ERROR! Incomplete response: %s' % data)

    parsed_result = {}
    for k in ['d1', 'd2', 'agr']:
        parsed_result[k] = data[k]

    for k in ['symbol', 'name', 'shortname', 'full_name', 'forecast', 'unit',
              'decimals', 'frequency', 'type', 'allowed_candles']:
        parsed_result[k] = data.get('series')[0][k]

    parsed_result['len(data_series)'] = len(data.get('series')[0]['data'])

    parsed_df = pd.DataFrame(data.get('series')[0]['data'])
    parsed_df['date'] = pd.to_datetime(parsed_df['date']).astype('datetime64[D]')
    for col in ['symbol', 'full_name', 'unit', 'type']:
        parsed_df[col] = parsed_result[col]
    parsed_result['parsed_df'] = parsed_df[['date', 'full_name', 'symbol', 'type',
                                            'x', 'y', 'change', 'percentChange']]
    return parsed_result

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    year = date.today().year if len(sys.argv) == 1 else int(sys.argv[1])

    symbols_df = pd.read_excel(os.path.join(CONFIG_DIR, f'symbols.xlsx'), sheet_name='symbols')
    symbols_df.dropna(inplace=True)
    symbols_df.reset_index(drop=True, inplace=True)
    auth_key_df = pd.read_excel(os.path.join(CONFIG_DIR, f'symbols.xlsx'), sheet_name='auth')
    auth_key_str = auth_key_df.loc[auth_key_df['source'] == 'commodities']['auth_key'].values[0]

    full_data_df = pd.DataFrame()
    for idx, row in symbols_df.iterrows():
        print('%s:' % row['full_name'], end=' ')
        sys.stdout.flush()
        parsed_result = get_hist(row['symbol'], row['category'], year,
                                 auth_key_str=auth_key_str, verbose=False)
        if parsed_result is None:
            print('No data found')
            continue
        print('%d / (%s - %s)' % (parsed_result['parsed_df'].shape[0],
                                  parsed_result['parsed_df']['date'].astype(str).values[0],
                                  parsed_result['parsed_df']['date'].astype(str).values[-1]
                                  ))
        assert row['full_name'] == parsed_result['full_name'], \
            'full_name not matching %s/%s' % (row['full_name'], parsed_result['full_name'])

        full_data_df = pd.concat([full_data_df, parsed_result['parsed_df']])

    full_data_df.to_csv(os.path.join(OUTPUT_DIR, f'commodities-{year}.csv'), index=False)
    print('\nDownload complete. full_data_df: %d symbols, shape: %s' %
          (len(full_data_df['symbol'].unique()), full_data_df.shape))