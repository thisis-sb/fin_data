"""
Download CF FRs & create JSON/XBRL archives & downloaded_metadata
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import sys
import glob
from datetime import datetime
import traceback
import pandas as pd
import pygeneric.misc as pyg_misc
from pygeneric import archiver, http_utils, datetime_utils
from fin_data.ind_cf import base_utils

PATH_1 = os.path.join(DATA_ROOT, '02_ind_cf/01_nse_fr_filings')
PATH_2 = os.path.join(DATA_ROOT, '02_ind_cf/02_nse_fr_archive')

''' --------------------------------------------------------------------------------------- '''
class DownloadManagerNSE:
    def __init__(self, year, verbose=False):
        self.verbose = verbose
        self.year = year

        self.checkpoint_interval = 200
        self.json_archives = {}
        self.xbrl_archives = {}
        self.download_metadata = {}
        self.session_errors = 0
        self.http_obj = None

        self.timestamp = datetime.today().strftime('%Y-%m-%d-%H-%M')
        self.json_base_url = 'https://www.nseindia.com/api/corporates-financial-results-data?index=equities&'

        self.downloaded_data_filename = os.path.join(PATH_2, f'downloads_{year}.csv')

    def download(self, max_downloads=1000):
        cf_fr_filename = os.path.join(PATH_1, 'CF_FR_%d.csv' % self.year)
        cf_frs = pd.read_csv(cf_fr_filename)
        print('\nLoaded %s, shape %s' % (cf_fr_filename, cf_frs.shape))

        cf_frs['key'] = cf_frs.apply(lambda x: base_utils.prepare_json_key(x), axis=1)
        if os.path.exists(self.downloaded_data_filename):
            df = pd.read_csv(self.downloaded_data_filename)
            print('Loaded %s, shape %s' % (self.downloaded_data_filename, df.shape))
            self.download_metadata = dict(zip(df['key'], df.to_dict('records')))
            cf_frs = cf_frs.loc[~cf_frs['key'].isin(df['key'].unique())].reset_index(drop=True)

        if cf_frs.shape[0] == 0:
            print('Nothing to download.')
            return

        print('\nDownloadManagerNSE initialized. To download: %d' % cf_frs.shape[0])
        t = datetime_utils.elapsed_time('DownloadManagerNSE.download')
        self.http_obj = http_utils.HttpDownloads(max_tries=10, timeout=30)
        n_downloaded, self.session_errors = 0, 0
        for idx in cf_frs.index:
            pyg_misc.print_progress_str(idx + 1, cf_frs.shape[0])

            cf_fr_row = cf_frs.loc[idx].to_dict()
            if self.download_one(cf_fr_row):
                n_downloaded += 1
                if n_downloaded > 0 and n_downloaded % self.checkpoint_interval == 0:
                    self.flush_all()
                    print('\n--> flush_all: full_metadata size: %d, n_downloaded/session_errors: %d/%d'
                          % (len(self.download_metadata), n_downloaded, self.session_errors))
                if n_downloaded >= max_downloads:
                    break
        self.flush_all()
        t = datetime_utils.elapsed_time('DownloadManagerNSE.download')

        print('\nDownloads finished --> n_downloaded/session_errors: %d/%d' % (n_downloaded, self.session_errors))
        print('time taken: %.2f seconds for %d downloads, %.3f seconds/record' % (t, n_downloaded, t / n_downloaded))
        df = pd.read_csv(self.downloaded_data_filename)
        print('saved_metadata: %d rows, %d errors' % (df.shape[0], df.loc[~df['json_outcome']].shape[0]))

        return

    def download_one(self, cf_fr_row):
        ''' just download here. all error checks are done during pre-processing '''
        key = cf_fr_row['key']
        period_end = datetime.strptime(cf_fr_row['toDate'], '%d-%b-%Y').strftime('%Y-%m-%d')

        ''' Step 1: download json_data from json_url & add it to json_archive '''
        json_url = self.json_base_url + key
        json_download_outcome = {'json_outcome': False, 'json_size': 0,
                                 'json_archive_path': None, 'json_error': ''}
        try:
            json_data, raw_data = self.http_obj.http_get_both(json_url)
            json_archive_file_name = '%d/json_period_end_%s' % (int(period_end[0:4]), period_end)
            if period_end not in self.json_archives.keys():
                f = os.path.join(PATH_2, json_archive_file_name)
                update = os.path.exists(f)
                self.json_archives[period_end] = archiver.Archiver(f, mode='w', update=update)
            self.json_archives[period_end].add(key, raw_data)

            json_download_outcome['json_outcome'] = True
            json_download_outcome['json_size'] = len(raw_data)
            json_download_outcome['json_archive_path'] = json_archive_file_name
        except Exception as e:
            err_msg = 'http_get_both failed %s\n%s' % (e, traceback.format_exc())
            json_outcome['json_error'] = err_msg
            self.session_errors += 1
            ''' should we return here? not sure. '''

        ''' Step 2: download xbrl_data from xbrl_linl & add it to xbrl_archive'''
        xbrl_link = cf_fr_row['xbrl']
        xbrl_download_outcome = {'xbrl_outcome': False, 'xbrl_size': 0,
                                 'xbrl_archive_path': None, 'xbrl_error': ''}
        if os.path.basename(xbrl_link) == '-':
            xbrl_download_outcome['xbrl_error'] = 'invalid xbrl ink: [%s]' % xbrl_link
        else:
            try:
                xbrl_data = self.http_obj.http_get(xbrl_link)
                if len(xbrl_data) == 0:
                    xbrl_download_outcome['xbrl_error'] = 'empty xbrl_data'
                else:
                    xbrl_archive_file_name = '%d/xbrl_period_end_%s' % (int(period_end[0:4]), period_end)
                    if period_end not in self.xbrl_archives.keys():
                        f = os.path.join(PATH_2, xbrl_archive_file_name)
                        update = os.path.exists(f)
                        self.xbrl_archives[period_end] = archiver.Archiver(f, mode='w', update=update)
                    self.xbrl_archives[period_end].add(xbrl_link, xbrl_data)

                    xbrl_download_outcome['xbrl_outcome'] = True
                    xbrl_download_outcome['xbrl_size'] = len(xbrl_data)
                    xbrl_download_outcome['xbrl_archive_path'] = xbrl_archive_file_name
            except Exception as e:
                err_msg = 'http_get failed:\n%s\n%s' % (e, traceback.format_exc())
                xbrl_download_outcome['xbrl_error'] = err_msg

        ''' Step 3: store mininum info here. '''
        ''' Final metadata is prepared during processing & stored in a metadata_{year}.csv '''
        self.download_metadata[key] = {
            'symbol': cf_fr_row['symbol'],
            'download_timestamp': self.timestamp,
            'key': key,
            'json_url': json_url,
            'json_outcome': json_download_outcome['json_outcome'],
            'json_size': json_download_outcome['json_size'],
            'json_archive_path': json_archive_file_name,
            'json_error': json_download_outcome['json_error'],
            'xbrl_link': cf_fr_row['xbrl'],
            'xbrl_outcome': xbrl_download_outcome['xbrl_outcome'],
            'xbrl_size': xbrl_download_outcome['xbrl_size'],
            'xbrl_archive_path': xbrl_download_outcome['xbrl_archive_path'],
            'xbrl_error': xbrl_download_outcome['xbrl_error']
        }

        return  json_download_outcome['json_outcome']

    def flush_all(self):
        for k in self.json_archives.keys():
            self.json_archives[k].flush(create_parent_dir=True)

        for k in self.xbrl_archives.keys():
            self.xbrl_archives[k].flush(create_parent_dir=True)

        df = pd.DataFrame(list(self.download_metadata.values()))
        df.sort_values(by='download_timestamp', inplace=True)
        df.to_csv(self.downloaded_data_filename, index=False)

        self.json_archives.clear()
        self.xbrl_archives.clear()

        return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-y", help='calendar year')
    arg_parser.add_argument("-sy", nargs='+', help="list of nse symbols")
    arg_parser.add_argument("-md", type=int, default=1000, help="max downloads")
    arg_parser.add_argument('-v', action='store_true', help="Verbose")
    args = arg_parser.parse_args()

    print('\nArgs: year: %s, -sy: %s, md: %d, verbose: %s' %
          (args.y, args.sy, args.md, args.v))

    year = datetime.today().year if args.y is None else int(args.y)
    n_downloads = 1000 if args.md is None else args.md
    mgr = DownloadManagerNSE(year=year, verbose=args.v)
    mgr.download(max_downloads=n_downloads)