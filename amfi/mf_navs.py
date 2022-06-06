# ----------------------------------------------------------------------------------------------
# Download MF NAV files from AMFI website
# Usage: dd-mmm-yyyy [dd-mmm-yyyy ...]
# ----------------------------------------------------------------------------------------------

import os
import sys
import pandas as pd
from datetime import datetime, timedelta

def get_raw_amfi_data(last_date):
    date_from = (datetime.strptime(last_date, '%d-%b-%Y') - timedelta(days=4)).strftime('%d-%b-%Y')
    url_base = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx'
    url = f'{url_base}?frmdt=%s&todt=%s' % (date_from, last_date)

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

# ----------------------------------------------------------------------------------------------
if __name__ == '__main__':
    assert len(sys.argv) > 1, '\n\nERROR! At-least one date in dd-mmm-yyyy format required'
    dates = sys.argv[1:]

    print()
    for date in dates:
        print('Downloading AMFI NAVs for:', date, end=' ... ')
        sys.stdout.flush()

        raw_navs = get_raw_amfi_data(date)

        year  = str(datetime.strptime(date, '%d-%b-%Y').year)
        month = int(datetime.strptime(date, '%d-%b-%Y').month)
        day   = int(datetime.strptime(date, '%d-%b-%Y').day)
        month = f'{month}' if month > 9 else f'0{month}'
        day   = f'{day}' if day > 9 else f'0{day}'

        OUT_DIR = os.getenv('DATA_DIR') + f'/01_fin_data/05_amfi/navs-{year}'
        os.makedirs(OUT_DIR, exist_ok=True)
        raw_navs.to_csv(OUT_DIR + f'/navs-{year}-{month}-{day}.csv', index=False)
        print('Done & Saved')