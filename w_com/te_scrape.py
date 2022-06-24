# --------------------------------------------------------------------------------------------
# Commodities price data scraping from tradingeconomics.com
# --------------------------------------------------------------------------------------------
import sys
import os
import pandas as pd
import requests
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common.utils
from settings import CONFIG_DIR, DATA_ROOT

MODULE = '06_w_com'

def get_hist(symbol, category, year, verbose=False):
    d1, d2 = f'{year}-01-01', f'{year}-12-31'
    url1 = 'https://markets.tradingeconomics.com/chart?s=%s' % symbol
    url2 = '&d1=%s&d2=%s&interval=1d&securify=new&url=%s' % (d1, d2, category)
    url3 = '&AUTH=vYBn7PlsPR6IitxFXBIjCJ%2FUtI0%2Bs3EmOs4%2FNhC81V6ZNEIIuCBwoEYjWko7erlG'
    url4 = '&ohlc=0'
    url = url1 + url2 + url3 + url4
    if verbose: print('url:[%s]' % url)

    r = requests.get(url, headers=common.utils.http_request_header(), stream=True)
    if not r.ok:
        raise ValueError('ERROR!', r.ok)
    data = r.json()
    if data is None:
        return None

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
    # print(parsed_df[['symbol', 'full_name', 'type', 'date', 'y', 'unit']].tail())
    return parsed_result

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    year = 2022 if len(sys.argv) == 1 else int(sys.argv[1])

    symbols_df = pd.read_excel(CONFIG_DIR + f'/{MODULE}/symbols.xlsx', sheet_name='tradingeconomics')
    symbols_df.dropna(inplace=True)
    symbols_df.reset_index(drop=True)

    full_data_df = pd.DataFrame()
    for idx, row in symbols_df.iterrows():
        print('%s:' % row['full name'], end=' ')
        sys.stdout.flush()
        parsed_result = get_hist(row['symbol'], row['category'], year, verbose=False)
        if parsed_result is None:
            print('No data found')
            continue
        print('%d / %s' % (parsed_result['parsed_df'].shape[0],
                           parsed_result['parsed_df']['date'].astype(str).values[-1]
                           ))
        assert row['full name'] == parsed_result['full_name'], \
            'full_name not matching %s/%s' % (row['full name'], parsed_result['full_name'])

        full_data_df = pd.concat([full_data_df, parsed_result['parsed_df']])

    full_data_df.to_csv(DATA_ROOT + f'/{MODULE}/tradingeconomics-{year}.csv', index=False)
    print('full_data_df: %d symbols, shape: %s' %
          (len(full_data_df['symbol'].unique()), full_data_df.shape))
