# --------------------------------------------------------------------------------------------
# Download daily reports from NSE
# Usage: MMMYYYY
# --------------------------------------------------------------------------------------------

import datetime
import os
import sys
import requests
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import common.utils
from settings import DATA_ROOT, NSE_ARCHIVES_URL

OUTPUT_DIR     = os.path.join(DATA_ROOT, '01_nse_pv/02_dr')

def nse_download_file(url, destination_filename):
    r = requests.get(url, headers=common.utils.http_request_header(), stream=True)
    if r.ok:
        file_size = len(r.content)
        with open(destination_filename, 'wb') as f:
            downloaded_bytes = 0
            for chunk in r.iter_content(chunk_size=1024*8):
                if chunk:
                    f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
                    downloaded_bytes += len(chunk)
        if file_size != downloaded_bytes:
            # print('Error: file_size: %d, downloaded_bytes: %d' % (file_size, downloaded_bytes))
            return False, 'possibly corrupted'
    else:
        # print("Error: status code {}\n{}".format(r.status_code, r.text))
        return False, 'not downloaded'

    return 1, 'OK'

def nse_download_daily_reports(year_str, month_str, report_type, verbose=False):
    days   = {'JAN':31, 'FEB':28, 'MAR': 31, 'APR':30, 'MAY':31, 'JUN':30,
              'JUL':31, 'AUG':31, 'SEP': 30, 'OCT':31, 'NOV':30, 'DEC':31}
    months = {'JAN':1, 'FEB':2, 'MAR': 3, 'APR':4, 'MAY':5, 'JUN':6,
              'JUL':7, 'AUG':8, 'SEP': 9, 'OCT':10, 'NOV':11, 'DEC':12}
    if int(year_str) % 4 == 0: days['FEB'] = 29

    date_now = datetime.datetime.now()
    if date_now.year == int(year_str) and date_now.month == months[month_str]:
        n_days = date_now.day
        n_days_msg = '%s-%s: current month, %d days' % (year_str, month_str, n_days)
    else:
        n_days = days[month_str]
        n_days_msg = '%s-%s: some month in the past, %d days' % (year_str, month_str, n_days)

    destination_folder = OUTPUT_DIR + '/%s/%s' % (year_str, f'{months[month_str]}'.zfill(2))
    # print(destination_folder)
    Path(destination_folder).mkdir(parents=True, exist_ok=True)

    def get_one_file(sub_url, filename):
        url = '%s/%s/%s' % (NSE_ARCHIVES_URL, sub_url, filename)
        if verbose: print(f'    {filename} ... ', end='')
        result = nse_download_file(url, os.path.join(destination_folder, filename))
        if verbose: print('done') if result[0] else print(result[1])
        return result[0]

    if report_type == 'cm':
        print(f'\nDownloading Cash Market Reports ({n_days_msg}) ...')
        print('  Cash Market Bhavcopy ...')
        # https://archives.nseindia.com/content/historical/EQUITIES/2022/FEB/cm24FEB2022bhav.csv.zip
        sub_url = f'content/historical/EQUITIES/{year_str}/{month_str}'
        files_downloaded, last_filename = 0, None
        for d in range(1, n_days + 1):
            filename = 'cm%s%s%sbhav.csv.zip' % (f'{d}'.zfill(2), month_str, year_str)
            downloaded = get_one_file(sub_url, filename)
            if downloaded:
                files_downloaded += 1
                last_filename = filename
        print('  %d files downloaded, last: %s' % (files_downloaded, last_filename))

        print('  Security Wise Delivery Position - Compulsory Rolling Settlement ...')
        # https://archives.nseindia.com/archives/equities/mto/MTO_17032022.DAT
        sub_url = f'archives/equities/mto'
        files_downloaded, last_filename = 0, None
        for d in range(1, n_days + 1):
            filename = 'MTO_%s%s%s.DAT' % (f'{d}'.zfill(2), f'{months[month_str]}'.zfill(2), year_str)
            downloaded = get_one_file(sub_url, filename)
            if downloaded:
                files_downloaded += 1
                last_filename = filename
        print('  %d files downloaded, last: %s' % (files_downloaded, last_filename))

        print('  CM 52 WK H/L ...')
        # https://archives.nseindia.com/content/CM_52_wk_High_low_25012022.csv
        sub_url = 'content'
        files_downloaded, last_filename = 0, None
        for d in range(1, n_days + 1):
            filename = 'CM_52_wk_High_low_%s%s%s.csv' % \
                       (f'{d}'.zfill(2), f'{months[month_str]}'.zfill(2), year_str)
            downloaded = get_one_file(sub_url, filename)
            if downloaded:
                files_downloaded += 1
                last_filename = filename
        print('  %d files downloaded, last: %s' % (files_downloaded, last_filename))
    elif report_type == 'fao':
        print(f'\nDownloading Futures & Options Reports ({n_days_msg}) ...')
        print('  FAO Bhavcopy ...')
        # https://archives.nseindia.com/content/historical/DERIVATIVES/2022/JAN/fo25JAN2022bhav.csv.zip
        sub_url = f'content/historical/DERIVATIVES/{year_str}/{month_str}'
        files_downloaded, last_filename = 0, None
        for d in range(1, n_days + 1):
            filename = 'fo%s%s%sbhav.csv.zip' % (f'{d}'.zfill(2), month_str, year_str)
            downloaded = get_one_file(sub_url, filename)
            if downloaded:
                files_downloaded += 1
                last_filename = filename
        print('  %d files downloaded, last: %s' % (files_downloaded, last_filename))

        print('  FAO participant wise Open Interest ...')
        # https://archives.nseindia.com/content/nsccl/fao_participant_oi_27012022.csv
        sub_url = 'content/nsccl'
        files_downloaded, last_filename = 0, None
        for d in range(1, n_days + 1):
            filename = 'fao_participant_oi_%s%s%s.csv' % \
                       (f'{d}'.zfill(2), f'{months[month_str]}'.zfill(2), year_str)
            downloaded = get_one_file(sub_url, filename)
            if downloaded:
                files_downloaded += 1
                last_filename = filename
        print('  %d files downloaded, last: %s' % (files_downloaded, last_filename))

        print('  FAO participant wise Trading Volumes ...')
        # https://archives.nseindia.com/content/nsccl/fao_participant_vol_27012022.csv
        sub_url = 'content/nsccl'
        files_downloaded, last_filename = 0, None
        for d in range(1, n_days + 1):
            filename = 'fao_participant_vol_%s%s%s.csv' % \
                       (f'{d}'.zfill(2), f'{months[month_str]}'.zfill(2), year_str)
            downloaded = get_one_file(sub_url, filename)
            if downloaded:
                files_downloaded += 1
                last_filename = filename
        print('  %d files downloaded, last: %s' % (files_downloaded, last_filename))
    else:
        print('ERROR! Unknown report type')
        assert False

    return

if __name__ == '__main__':
    months = ['AUG2022'] if len(sys.argv) == 1 else sys.argv[1:]

    for m in months:
        assert m[0:3] in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                          'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'], f'Invalid month:{m[0:3]}'
        assert 2015 <= int(m[3:]) <= 2025, f'Invalid year:{m[3:]}'
        nse_download_daily_reports(m[3:], m[0:3], 'cm')
        nse_download_daily_reports(m[3:], m[0:3], 'fao')