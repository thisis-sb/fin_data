"""
To get select legacy data. Workaround for now as NSE doesn't have this data easily available
"""
''' --------------------------------------------------------------------------------------- '''

import os
from pathlib import Path
import datetime
import pandas as pd
import yfinance as yf

OUTPUT_DIR = os.path.join(os.getenv('DATA_ROOT'), '01_nse_pv/02_dr')
symbols_dict = {'NIFTY 50': '^NSEI'}

''' --------------------------------------------------------------------------------------- '''
def yf_api_func(symbol, year):
    assert year < 2018, '%d is not a legacy year' % year
    dt_s = datetime.date(year, 1, 1)
    dt_e = datetime.date(year, 12, 31)
    df   = yf.download(symbols_dict[symbol], start=dt_s, end=dt_e, progress=False)
    df.reset_index(inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    for c in ['Open', 'High', 'Low', 'Close', 'Adj Close']:
        df[c] = round(df[c], 2)
    return df

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    idx_symbol = 'NIFTY 50'
    parent_dir = os.path.join(OUTPUT_DIR, 'legacy/%s' % idx_symbol)

    for y in range(2008, 2018):
        print ('Getting legacy index data for %s for year %d ... ' % (idx_symbol, y), end='')
        pv_data = yf_api_func(idx_symbol, y, )
        Path(parent_dir).mkdir(parents=True, exist_ok=True)
        output_file_name = os.path.join(parent_dir, '%d.csv' % y)
        pv_data.to_csv(output_file_name, index=False)
        print('Done. Shape:', pv_data.shape)