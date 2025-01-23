"""
Download NSE Historical Price-Volume data from NSE website & apply corporate actions
Usage: symbols
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import sys
import datetime
import glob
import re
import ast
import pandas as pd
from time import sleep
import pygeneric.http_utils as http_utils

OUTPUT_DIR = os.path.join(DATA_ROOT, '01_nse_pv/01_api')
http_obj = None

''' --------------------------------------------------------------------------------------- '''
def get_raw_hpv_for_year(symbol, year, overwrite=False, verbose=False):
    date_today = datetime.date.today()
    output_filename = os.path.join(OUTPUT_DIR, f'{symbol}/raw', f'{year}.csv')

    if verbose:
        print('For %s for year %d ...' % (symbol, year), end=' ')
    if date_today.year != year and os.path.exists(output_filename):
        if overwrite:
            if verbose:
                print('some data exists, overwriting!')
        else:
            if verbose:
                print('data already downloaded')
            return

    from_date = datetime.date(year, 1, 1)
    end_date = date_today if year == date_today.year else datetime.date(year, 12, 31)

    global http_obj
    if http_obj is None:
        http_obj = http_utils.HttpDownloads(website='nse')

    results = []
    while from_date <= end_date:
        to_date = from_date + datetime.timedelta(100)
        if to_date > end_date:
            to_date = end_date
        if from_date == to_date:
            break
        from_date_str = '%s-%s-%d' % (('%s' % from_date.day).zfill(2),
                                      ('%s' % from_date.month).zfill(2),
                                      year)
        to_date_str = '%s-%s-%d' % (('%s' % to_date.day).zfill(2),
                                    ('%s' % to_date.month).zfill(2),
                                    year)
        # print('-->', from_date_str, to_date_str)
        url = 'https://www.nseindia.com/api/historical/securityArchives?' + \
              'from=%s&to=%s&symbol=%s' % (from_date_str, to_date_str, symbol) + \
              '&dataType=priceVolumeDeliverable&series=EQ'
        get_dict = http_obj.http_get_json(url)

        df = pd.DataFrame(get_dict['data'])
        if df.shape[0] > 0:
            results.append(df)
            if verbose:
                print('  Retrieved -->', df['CH_TIMESTAMP'].values[0], df['CH_TIMESTAMP'].values[-1])
            from_date = datetime.datetime.strptime(
                df['CH_TIMESTAMP'].values[-1], '%Y-%m-%d').date() + datetime.timedelta(1)
        else:
            from_date = from_date + datetime.timedelta(100)

        if from_date > end_date:
            break

    if len(results) == 0:
        print('  No data for year %d' % year)
        return True

    r_df = pd.concat([x for x in results]).drop_duplicates('CH_TIMESTAMP')
    r_df = r_df.sort_values(by='CH_TIMESTAMP').reset_index(drop=True)
    print('  Retrieved for year %d --> %d rows, from / to: %s / %s' %
          (year, r_df.shape[0], r_df['CH_TIMESTAMP'].values[0], r_df['CH_TIMESTAMP'].values[-1]))

    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    r_df.to_csv(output_filename, index=False)

    return True

def get_raw_hpv_clean_raw(symbol, years, overwrite=False, verbose=False):
    print('get_raw_hpv_clean_raw: %s for years %s' % (symbol, years))
    """[get_raw_hpv_for_year(symbol, y, verbose)
     for y in range(years, datetime.date.today().year + 1)]"""

    [get_raw_hpv_for_year(symbol, y, overwrite=overwrite, verbose=verbose) for y in years]
    raw_files = glob.glob(os.path.join(OUTPUT_DIR, f'{symbol}/raw/*.csv'))
    raw_data = pd.concat([pd.read_csv(f) for f in raw_files])

    cols_dict = {'CH_TIMESTAMP':'Date', 'CH_SYMBOL':'Symbol', 'CH_SERIES':'Series',
                 'CH_OPENING_PRICE':'Open', 'CH_TRADE_HIGH_PRICE':'High',
                 'CH_TRADE_LOW_PRICE':'Low', 'CH_CLOSING_PRICE':'Close',
                 'CH_PREVIOUS_CLS_PRICE':'Prev Close', 'CH_LAST_TRADED_PRICE':'Last', 'VWAP':'VWAP',
                 'CH_52WEEK_HIGH_PRICE':'52_Wk_H', 'CH_52WEEK_LOW_PRICE':'52_Wk_L',
                 'CH_TOT_TRADED_QTY':'Volume', 'CH_TOT_TRADED_VAL':'Traded Value',
                 'CH_TOTAL_TRADES':'No Of Trades',
                 'COP_DELIV_QTY':'Delivery Volume', 'COP_DELIV_PERC':'% Deliverble', 'CA':'CA'}

    raw_data = raw_data[list(cols_dict.keys())]
    raw_data = raw_data.rename(columns=cols_dict)
    raw_data = raw_data.sort_values(by='Date').reset_index(drop=True)
    return raw_data

def process_ca(raw_pv, verbose=False):
    xx = raw_pv[['Date', 'CA']].copy()
    corp_actions = xx.dropna()
    xx['mult'] = 1.0
    # print(corp_actions)
    if verbose:
        corp_actions.to_csv(os.path.join(LOG_DIR, 'corp_actions.csv'))

    for idx, row in corp_actions.iterrows():
        to_index = idx

        if verbose:
            print('XXX -->', ast.literal_eval('%s' % row['CA'])[0]['subject'])
        ca_subject = ast.literal_eval('%s' % row['CA'])[0]['subject'].strip()

        if ca_subject[0:16] == 'Face Value Split':
            tok = re.split('Rs | Re ', ca_subject)
            try:
                mult = float(tok[2].split('/-')[0]) / float(tok[1].split('/-')[0])
            except:
                try:
                    mult = float(tok[2].split('Per')[0]) / float(tok[1].split('Per')[0])
                except:
                    assert False, 'ca_subject: [%s] [%s]' % (row['Date'], ca_subject)
            if verbose:
                print('mult:', mult)
            xx.loc[xx.index < idx, 'mult'] = mult * xx.loc[xx.index < idx, 'mult']
        elif ca_subject[0:5] == 'Bonus':
            tok = ca_subject.split()[1].split(':')
            mult = float(tok[1]) / (float(tok[0]) + float(tok[1]))
            if verbose:
                print('mult:', mult)
            xx.loc[xx.index < idx, 'mult'] = mult * xx.loc[xx.index < idx, 'mult']
        else:
            if verbose:
                print('Ignoring: %s / %s' % (row['Date'], ca_subject))

    xx['ca_chk'] = xx.mult.eq(xx.mult.shift())
    if verbose:
        print('process_ca mult xx df:')
        print(xx.loc[~xx.ca_chk][1:])
    return xx

def get_pv_data(symbol, after=None, from_to=None, n_days=0):
    API_DATA_PATH = os.path.join(DATA_ROOT, '01_nse_pv/01_api')
    df = pd.read_csv(os.path.join(API_DATA_PATH, f'{symbol}/pv_data_adjusted.csv'))
    df['Date'] = df['Date'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
    df.drop_duplicates(inplace=True)
    df = df.sort_values(by='Date').reset_index(drop=True)

    if after is not None:
        df = df.loc[df['Date'] >= datetime.datetime.strptime(after, '%Y-%m-%d')].reset_index(drop=True)
    elif from_to is not None:
        date1 = datetime.datetime.strptime(from_to[0], '%Y-%m-%d')
        date2 = datetime.datetime.strptime(from_to[1], '%Y-%m-%d')
        df = df.loc[(df.Date >= date1) & (df.Date <= date2)].reset_index(drop=True)
    elif n_days != 0:
        df = df.tail(n_days).reset_index(drop=True)

    return df

def wrapper(symbols=None, year=None, overwrite=False, verbose=False):
    if symbols is None:
        symbols = ['ASIANPAINT', 'BRITANNIA', 'HDFCBANK', 'ICICIBANK', 'IRCON',
                   'IRCTC', 'JUBLFOOD', 'NMDC', 'TATASTEEL', 'ZYDUSLIFE']

    years = list(range(2018, datetime.date.today().year + 1)) if year is None else [year]

    for symbol in symbols:
        raw_pv_data = get_raw_hpv_clean_raw(symbol, years, overwrite=overwrite, verbose=verbose)
        print('Loaded raw_pv_data.shape: %s, from / to: %s / %s' %
              (raw_pv_data.shape, raw_pv_data['Date'].values[0], raw_pv_data['Date'].values[-1]))
        ca_mults = process_ca(raw_pv_data, verbose=False)  # use verbose only when there's a bug
        pv_data = pd.merge(raw_pv_data, ca_mults[['Date', 'mult']], on='Date', how='left')
        for c in ['Open', 'High', 'Low', 'Close', 'Prev Close', 'Last',
                  'VWAP', '52_Wk_H', '52_Wk_L']:
            pv_data[c] = round(pv_data[c] * pv_data['mult'], 2)
        for c in ['Traded Value']:
            pv_data[c] = pv_data[c] * 100000
        output_filename = os.path.join(OUTPUT_DIR, f'{symbol}/pv_data_adjusted.csv')
        pv_data.to_csv(output_filename, index=False)
        print('Coroporate Actions applied, saved to %s\n' %
              os.path.join(*(output_filename.split('\\')[5:])))

    return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument('-sy', nargs='+', help="nse symbols")
    arg_parser.add_argument('-y', type=int, help="year")
    arg_parser.add_argument('-o', action='store_true', help="overwrite if already downloaded")
    arg_parser.add_argument('-v', action='store_true', help="verbose")
    args = arg_parser.parse_args()

    if args.sy is None:
        arg_parser.print_help()
        print('\n--> args.sy is None. Will be using default set of symbols\n')

    wrapper(symbols=args.sy, year=args.y, overwrite=args.o, verbose=args.v)