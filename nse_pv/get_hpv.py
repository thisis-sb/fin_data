# --------------------------------------------------------------------------------------------
# NSE Historical Price-Volume & Price-Ratios download
# Usage: [full | ytd] [csv | symbols]
# --------------------------------------------------------------------------------------------

import datetime
import os
import sys
import pandas as pd
import nsepy
from time import sleep
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from global_env import DATA_ROOT

OUTPUT_DIR     = DATA_ROOT + '/01_nse_pv/01_api'

# --------------------------------------------------------------------------------------------
def fetch_year_data(symbol, symbol_type, year, verbose=False):
    if verbose: print(f'  {symbol} for year {year}', end=' ')
    date_today_tokens = datetime.datetime.now().strftime('%Y-%m-%d').split('-')
    start_date = datetime.date(year, 1, 1)
    end_date   = datetime.date(year, 12, 31) if year < int(date_today_tokens[0]) else \
        datetime.date(year, int(date_today_tokens[1]), int(date_today_tokens[2]))

    if symbol_type == 'IDX':
        df = nsepy.get_history(symbol=symbol, start=start_date, end=end_date, index=True)
    elif symbol_type == 'EQ':
        df = nsepy.get_history(symbol=symbol, start=start_date, end=end_date)
    else:
        assert False, "ERROR: Unknown option, ... exiting with error"

    if df.shape[0] > 0:
        output_filename = OUTPUT_DIR + '/' + symbol + f'/{symbol}-{year}.csv'
        if not os.path.exists(os.path.dirname(output_filename)):
            os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        df.to_csv(output_filename)
        if verbose: print(df.shape[0], 'rows.')
    else:
        if verbose: print('no data.')

    last_date = None if df.shape[0] == 0 else df.index[-1]

    return df.shape[0], last_date

# --------------------------------------------------------------------------------------------
def fetch_symbol_data(symbol, symbol_type, download_type, verbose=False):
    print(f'{symbol} ({symbol_type} {download_type}): ', end=''); sys.stdout.flush()
    date_today_tokens = datetime.datetime.now().strftime('%Y-%m-%d').split('-')
    n_rows = 0
    years = range(2001, int(date_today_tokens[0]) + 1) if download_type == 'full' else \
        [int(date_today_tokens[0])]

    last_date = None
    for year in years:
        nr, last_date = fetch_year_data(symbol, symbol_type, year, verbose)
        n_rows += nr

    print(f'{n_rows} rows fetched, last_date = {last_date}')
    return

# --------------------------------------------------------------------------------------------
def download_all(symbols, download_type, verbose=False):
    if len(symbols) == 1 and symbols[0][-4:] == '.csv':
        csv_file = pd.read_csv(os.path.join(os.getenv('DATA_DIR'),
                                            '00_config\\fin_data_symbols', symbols[0]))
        col_list = csv_file.columns.to_list()
        if 'Group' in col_list:
            symbol_list = list(csv_file['Symbol'] + '.' + csv_file['Group'])
        else:
            symbol_list = [sym + '.NSE EQ' for sym in csv_file['Symbol'].to_list()]
    else:
        symbol_list = symbols

    symbol_list = list(set(symbol_list))
    symbol_list.sort()

    for i, sym in enumerate(symbol_list):
        if sym.endswith('.IDX'):
            fetch_symbol_data(sym.split('.')[0], 'IDX', download_type, verbose)
        else:
            fetch_symbol_data(sym.split('.')[0], 'EQ', download_type, verbose)
        if i < len(symbol_list)-1:
            sleep(3)
    print(f'\nData for {len(symbol_list)} symbols retrieved.')
    return

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    verbose = False
    download_type = 'ytd' if len(sys.argv) == 1 else sys.argv[1]
    symbols = sys.argv[2:] if len(sys.argv) > 2 else\
        ['NIFTY 50.IDX', 'NIFTY BANK.IDX', 'ASIANPAINT.EQ', 'BRITANNIA.EQ', 'HDFC.EQ',
         'ICICIBANK.EQ', 'IRCTC.EQ', 'JUBLFOOD.EQ', 'ZYDUSLIFE.EQ', 'PAYTM.EQ']

    print(f'\nDownload Type: {download_type}\nSymbols: {symbols} ::: \n')
    download_all(symbols, download_type, verbose)
