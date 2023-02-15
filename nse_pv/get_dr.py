"""
Download daily reports from NSE
Usage: MMMYYYY
"""
''' --------------------------------------------------------------------------------------- '''

import datetime
import os
import sys
import requests
from pathlib import Path
from calendar import monthrange, month_abbr
from pygeneric.archiver import Archiver
import pygeneric.http_utils as http_utils

NSE_ARCHIVES_URL = 'https://archives.nseindia.com'
OUTPUT_DIR = os.path.join(os.getenv('DATA_ROOT'), '01_nse_pv/02_dr')

''' --------------------------------------------------------------------------------------- '''
def get_files(sub_url, filenames, archive_full_path):
    archive = Archiver(archive_full_path, 'w', overwrite=True, compression='zip')
    n_downloaded, total_size, last_file = 0, 0, None
    for f in filenames:
        url = '%s/%s/%s' % (NSE_ARCHIVES_URL, sub_url, f)
        r = requests.get(url, headers=http_utils.http_request_header(), stream=True)
        if r.ok:
            n_downloaded += 1
            total_size += len(r.content)
            archive.add(f, r.content)
            last_file = f
    archive.flush()
    print('%d files, %.1f KB, last: %s' % (n_downloaded, total_size/1e3, last_file))

def nse_download_daily_reports(year_str, month_str):
    months_dict = {month.upper(): index for index, month in enumerate(month_abbr) if month}
    date_now = datetime.datetime.now()
    n_days = date_now.day \
        if (date_now.year == int(year_str) and date_now.month == months_dict[month_str]) else \
        monthrange(int(year_str), months_dict[month_str])[1]
    print('\nDownloading for %s-%s: n_days: %d ...' % (year_str, month_str, n_days))

    dest_folder = OUTPUT_DIR + '/%s/%s' % (year_str, f'{months_dict[month_str]}'.zfill(2))
    Path(dest_folder).mkdir(parents=True, exist_ok=True)

    print('\nCash Market reports ...')
    cm_reports = [
        # https://archives.nseindia.com/content/indices/ind_close_all_30092022.csv
        {'msg': '- Indices daily snapshot: ',
         'sub_url': 'content/indices',
         'archive': f'{dest_folder}/indices_close.zip',
         'files': ['ind_close_all_%s%s%s.csv'
                   % (f'{d}'.zfill(2), f'{months_dict[month_str]}'.zfill(2), year_str)
                   for d in range(1, n_days + 1)]
         },
        # https://archives.nseindia.com/content/historical/EQUITIES/2022/SEP/cm30SEP2022bhav.csv.zip
        {
            'msg': '- Cash market bhavcopy: ',
            'sub_url': f'content/historical/EQUITIES/{year_str}/{month_str}',
            'archive': f'{dest_folder}/cm_bhavcopy.zip',
            'files': ['cm%s%s%sbhav.csv.zip' % (f'{d}'.zfill(2), month_str, year_str)
                      for d in range(1, n_days + 1)]
        },
        # https://archives.nseindia.com/archives/equities/mto/MTO_30092022.DAT
        {
            'msg': '- Security Wise Delivery Position: ',
            'sub_url': 'archives/equities/mto',
            'archive': f'{dest_folder}/MTO.zip',
            'files': ['MTO_%s%s%s.DAT' %
                      (f'{d}'.zfill(2), f'{months_dict[month_str]}'.zfill(2), year_str)
                      for d in range(1, n_days + 1)]
        },
        # https://archives.nseindia.com/content/CM_52_wk_High_low_30092022.csv
        {
            'msg': '- CM 52 WK H/L: ',
            'sub_url': 'content',
            'archive': f'{dest_folder}/cm_52_wk_HL.zip',
            'files': ['CM_52_wk_High_low_%s%s%s.csv' %
                      (f'{d}'.zfill(2), f'{months_dict[month_str]}'.zfill(2), year_str)
                      for d in range(1, n_days + 1)]
        },
        # https://archives.nseindia.com/archives/equities/bhavcopy/pr/PR300922.zip
        {
            'msg': '- Consolidated CM bhavcopy: ',
            'sub_url': 'archives/equities/bhavcopy/pr',
            'archive': f'{dest_folder}/PR.zip',
            'files': ['PR%s%s%s.zip' %
                      (f'{d}'.zfill(2), f'{months_dict[month_str]}'.zfill(2), year_str[2:])
                      for d in range(1, n_days + 1)]
        }
    ]

    for i, r in enumerate(cm_reports):
        print(r['msg'], end='')
        get_files(r['sub_url'], r['files'], r['archive'])

    print('\nDerivatives reports ...')
    fo_reports = [
        # https://archives.nseindia.com/content/historical/DERIVATIVES/2022/SEP/fo30SEP2022bhav.csv.zip
        {
            'msg': '- FO bhavcopy: ',
            'sub_url': f'content/historical/DERIVATIVES/{year_str}/{month_str}',
            'archive': f'{dest_folder}/fo_bhavcopy.zip',
            'files': ['fo%s%s%sbhav.csv.zip' % (f'{d}'.zfill(2), month_str, year_str)
                      for d in range(1, n_days + 1)]
        },
        # https://archives.nseindia.com/content/nsccl/fao_participant_vol_30092022.csv
        {
            'msg': '- FO participant-wise OI: ',
            'sub_url': 'content/nsccl',
            'archive': f'{dest_folder}/fo_participant_wise_oi.zip',
            'files': ['fao_participant_vol_%s%s%s.csv' %
                      (f'{d}'.zfill(2), f'{months_dict[month_str]}'.zfill(2), year_str)
                      for d in range(1, n_days + 1)]
        },
        # https://archives.nseindia.com/content/nsccl/fao_participant_vol_30092022.csv
        {
            'msg': '- FO participant-wise trading volumes: ',
            'sub_url': 'content/nsccl',
            'archive': f'{dest_folder}/fo_participant_wise_vol.zip',
            'files': ['fao_participant_vol_%s%s%s.csv' %
                      (f'{d}'.zfill(2), f'{months_dict[month_str]}'.zfill(2), year_str)
                      for d in range(1, n_days + 1)]
        }
    ]

    for i, r in enumerate(fo_reports):
        print(r['msg'], end='')
        get_files(r['sub_url'], r['files'], r['archive'])

    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    months = [datetime.date.today().strftime('%b%Y').upper()] if len(sys.argv) == 1 else sys.argv[1:]

    for m in months:
        assert m[0:3] in ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
                          'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'], f'Invalid month:{m[0:3]}'
        assert 2015 <= int(m[3:]) <= 2025, f'Invalid year:{m[3:]}'
        nse_download_daily_reports(m[3:], m[0:3])