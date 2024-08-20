"""
Download MF NAV files from AMFI website
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import sys
import pandas as pd
from datetime import date, timedelta

''' --------------------------------------------------------------------------------------- '''
def get_raw_amfi_data(as_on_date):
    date_from = as_on_date - timedelta(days=7)
    url_base = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx'
    url = f'{url_base}?frmdt=%s&todt=%s' % (date_from.strftime('%d-%b-%Y'),
                                            as_on_date.strftime('%d-%b-%Y'))

    df = pd.read_csv(url, sep=';')
    assert df.shape[0] > 0, print('ERROR! get_raw_amfi_data: No raw data retrieved')

    raw_navs = df[df['Date'].notna()].reset_index(drop=True)
    assert raw_navs.shape[0] > 0, print('ERROR! get_raw_amfi_data: No valid data found')

    df.rename(columns={'Scheme Code': 'CODE',
                       'ISIN Div Payout/ISIN Growth': 'ISIN',
                       'Net Asset Value': 'NAV'
                       }, inplace=True)
    df = df[['CODE', 'ISIN', 'NAV', 'Date', 'Scheme Name']]

    df = df.groupby(['CODE', 'ISIN']).tail(1)

    return df

def eom_dates(year):
    dates = []
    for m in range(2,13):
        dates.append(date(year, m, 1) - timedelta(1))
    dates.append(date(year+1, 1, 1) - timedelta(1))
    dates = [d for d in dates if d <= date.today()]
    return dates

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser

    arg_parser = ArgumentParser()
    arg_parser.add_argument('-y', type=int, default=date.today().year, help="calendar year")
    args = arg_parser.parse_args()

    dates = eom_dates(args.y)

    for d in dates:
        print('  navs for %s:' % d.strftime('%d-%b-%Y'), end=' ')
        f = os.path.join(DATA_ROOT, '09_amfi/%d/%s.csv' % (d.year, d.strftime('%Y-%m-%d')))
        if os.path.isfile(f):
            print('already exists.')
        else:
            raw_navs = get_raw_amfi_data(d)
            os.makedirs(os.path.dirname(f), exist_ok=True)
            raw_navs.to_csv(f, index=False)
            print('downloaded & saved. shape:', raw_navs.shape)