"""
Download CF FRs & create JSON/XBRL archives & downloaded_metadata
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import sys
import glob
from pathlib import Path
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
        self.session_errors = 0
        self.http_obj = None

        self.dl_md_filename_json = os.path.join(PATH_2, f'dl_md/download_metadata_json_{year}.csv')
        self.dl_md_filename_xbrl = os.path.join(PATH_2, f'dl_md/download_metadata_xbrl_{year}.csv')
        self.json_data_archives = {}
        self.xbrl_data_archives = {}
        self.dl_md_json = {}
        self.dl_md_xbrl = {}

        self.base_url_json = 'https://www.nseindia.com/api/' \
                             'corporates-financial-results-data?index=equities&'

        self.timestamp = datetime.today().strftime('%Y-%m-%d-%H-%M')

        return

    def download(self, max_downloads=1000):
        self.__download__('json', max_downloads)
        self.__download__('xbrl', max_downloads)
        return

    def __download__(self, mode, max_downloads):
        assert mode in ['json', 'xbrl']
        print(f'\nStarting __download__ ({mode}): \n%s' % (90 * '-'))
        to_download = self.__what_to_download__(mode)

        if to_download.shape[0] == 0:
            print(f'__download__ ({mode}): Nothing to download.')
            return

        print('\nTo download (%s): %d (max_downloads: %d)' % (mode, to_download.shape[0], max_downloads))

        if self.http_obj is None:
            self.http_obj = http_utils.HttpDownloads(max_tries=10, timeout=30)
        t = datetime_utils.elapsed_time('DownloadManagerNSE.__download__')
        n_downloaded, self.session_errors = 0, 0

        for idx in to_download.index:
            pyg_misc.print_progress_str(idx + 1, to_download.shape[0])

            cf_fr_row = to_download.loc[idx].to_dict()
            if self.__download_one__(mode, cf_fr_row):
                n_downloaded += 1
                if n_downloaded > 0 and n_downloaded % self.checkpoint_interval == 0:
                    self.__flush__(mode)
                    md_file = self.dl_md_filename_json if mode == 'json' else self.dl_md_filename_xbrl
                    md_len  = len(self.dl_md_json) if mode == 'json' else len(self.dl_md_xbrl)
                    print('\n    --> flush: %s size: %d, n_downloaded/session_errors: %d/%d'
                          % (os.path.basename(md_file), md_len, n_downloaded, self.session_errors))
                if n_downloaded >= max_downloads:
                    break
        self.__flush__(mode)
        t = datetime_utils.elapsed_time('DownloadManagerNSE.__download__')
        print(f'\n\nDownloads ({mode}) completed. Summary:')

        print('  n_downloaded/session_errors: %d/%d' % (n_downloaded, self.session_errors))
        print('  time taken: %.2f seconds for %d downloads' % (t, n_downloaded))
        print('  --> %.3f seconds/record' % (t / n_downloaded))

        md_file = self.dl_md_filename_json if mode == 'json' else self.dl_md_filename_xbrl
        outcome_column = 'json_outcome' if mode == 'json' else 'xbrl_outcome'
        df = pd.read_csv(md_file)
        n_errors = df.loc[~df[outcome_column]].shape[0]
        print('  %s: %d rows, %d errors' % (os.path.basename(md_file), df.shape[0], n_errors))
        print(90 * '-')

        return

    def __download_one__(self, mode, cf_fr_row):
        assert mode in ['json', 'xbrl']
        return self.__download_one_json__(cf_fr_row) if mode == 'json' \
            else self.__download_one_xbrl__(cf_fr_row)

    def __download_one_json__(self, cf_fr_row):
        ''' just download & no pre-processibg. All error checks are done during pre-processing '''
        json_key = cf_fr_row['json_key']
        period_end = datetime.strptime(cf_fr_row['toDate'], '%d-%b-%Y').strftime('%Y-%m-%d')

        json_link = self.base_url_json + json_key
        json_download_outcome = {
            'json_outcome': False, 'json_size': 0, 'json_archive_path': None, 'json_error': ''
        }
        try:
            json_data, raw_data = self.http_obj.http_get_both(json_link)
            json_archive_path = '%d/json_data_period_end_%s' % (int(period_end[0:4]), period_end)
            if period_end not in self.json_data_archives.keys():
                f = os.path.join(PATH_2, json_archive_path)
                update = os.path.exists(f)
                self.json_data_archives[period_end] = archiver.Archiver(f, mode='w', update=update)
            self.json_data_archives[period_end].add(json_key, raw_data)

            json_download_outcome['json_outcome'] = True
            json_download_outcome['json_size'] = len(raw_data)
            json_download_outcome['json_archive_path'] = json_archive_path
        except Exception as e:
            err_msg = 'http_get_both failed %s\n%s' % (e, traceback.format_exc())
            json_download_outcome['json_error'] = err_msg
            self.session_errors += 1

        self.dl_md_json[json_key] = {
            'symbol': cf_fr_row['symbol'],
            'json_key': json_key,
            'json_outcome': json_download_outcome['json_outcome'],
            'json_size': json_download_outcome['json_size'],
            'json_archive_path': json_download_outcome['json_archive_path'],
            'json_error': json_download_outcome['json_error'],
            'timestamp': self.timestamp,
            'json_link': json_link
        }

        return  json_download_outcome['json_outcome']

    def __download_one_xbrl__(self, cf_fr_row):
        ''' just download & no pre-processibg. All error checks are done during pre-processing '''
        xbrl_link = cf_fr_row['xbrl']
        xbrl_key = os.path.basename(xbrl_link)
        period_end = datetime.strptime(cf_fr_row['toDate'], '%d-%b-%Y').strftime('%Y-%m-%d')

        xbrl_download_outcome = {
            'xbrl_outcome': False, 'xbrl_size': 0, 'xbrl_archive_path': None, 'xbrl_error': ''
        }
        if xbrl_key == '-':
            xbrl_download_outcome['xbrl_error'] = 'invalid xbrl_link: [%s]' % xbrl_link
        else:
            try:
                xbrl_data = self.http_obj.http_get(xbrl_link)
                if len(xbrl_data) == 0:
                    xbrl_download_outcome['xbrl_error'] = 'empty xbrl_data'
                else:
                    xbrl_archive_path = '%d/xbrl_data_period_end_%s' % (int(period_end[0:4]), period_end)
                    if period_end not in self.xbrl_data_archives.keys():
                        f = os.path.join(PATH_2, xbrl_archive_path)
                        update = os.path.exists(f)
                        self.xbrl_data_archives[period_end] = archiver.Archiver(f, mode='w', update=update)
                    self.xbrl_data_archives[period_end].add(xbrl_key, xbrl_data)

                    xbrl_download_outcome['xbrl_outcome'] = True
                    xbrl_download_outcome['xbrl_size'] = len(xbrl_data)
                    xbrl_download_outcome['xbrl_archive_path'] = xbrl_archive_path
            except Exception as e:
                err_msg = 'http_get failed:\n%s\n%s' % (e, traceback.format_exc())
                xbrl_download_outcome['xbrl_error'] = err_msg
                self.session_errors += 1

        self.dl_md_xbrl[xbrl_key] = {
            'symbol': cf_fr_row['symbol'],
            'xbrl_key': xbrl_key,
            'xbrl_outcome': xbrl_download_outcome['xbrl_outcome'],
            'xbrl_size': xbrl_download_outcome['xbrl_size'],
            'xbrl_archive_path': xbrl_download_outcome['xbrl_archive_path'],
            'xbrl_error': xbrl_download_outcome['xbrl_error'],
            'timestamp': self.timestamp,
            'xbrl_link': xbrl_link
        }

        return  xbrl_download_outcome['xbrl_outcome']

    def __flush__(self, mode):
        assert mode in ['json', 'xbrl']
        if mode == 'json':
            for k in self.json_data_archives.keys():
                self.json_data_archives[k].flush(create_parent_dir=True)
            df = pd.DataFrame(list(self.dl_md_json.values()))
            df.sort_values(by='timestamp', inplace=True)
            Path(os.path.dirname(self.dl_md_filename_json)).mkdir(parents=False, exist_ok=True)
            df.to_csv(self.dl_md_filename_json, index=False)
            self.json_data_archives.clear()
        elif mode == 'xbrl':
            for k in self.xbrl_data_archives.keys():
                self.xbrl_data_archives[k].flush(create_parent_dir=True)
            df = pd.DataFrame(list(self.dl_md_xbrl.values()))
            df.sort_values(by='timestamp', inplace=True)
            Path(os.path.dirname(self.dl_md_filename_xbrl)).mkdir(parents=False, exist_ok=True)
            df.to_csv(self.dl_md_filename_xbrl, index=False)
            self.xbrl_data_archives.clear()
        else:
            assert False, mode

        return True

    def __what_to_download__(self, mode):
        f = os.path.join(PATH_1, 'CF_FR_%d.csv' % self.year)
        to_download = pd.read_csv(f)
        print('Loaded %s, shape %s' % (os.path.basename(f), to_download.shape))

        if mode == 'json':
            to_download['json_key'] = to_download.apply(lambda x: base_utils.prepare_json_key(x), axis=1)
            if os.path.exists(self.dl_md_filename_json):
                df = pd.read_csv(self.dl_md_filename_json)
                print('Loaded %s, shape %s' % (os.path.basename(self.dl_md_filename_json), df.shape))
                self.dl_md_json = dict(zip(df['json_key'], df.to_dict('records')))
                to_download = to_download.loc[~to_download['json_key'].isin(df['json_key'].unique())]
        elif mode == 'xbrl':
            to_download['xbrl_key'] = to_download.apply(lambda x: os.path.basename(x['xbrl']), axis=1)
            if os.path.exists(self.dl_md_filename_xbrl):
                df = pd.read_csv(self.dl_md_filename_xbrl)
                print('Loaded %s, shape %s' % (os.path.basename(self.dl_md_filename_xbrl), df.shape))
                self.dl_md_xbrl = dict(zip(df['xbrl_key'], df.to_dict('records')))
                to_download = to_download.loc[~to_download['xbrl_key'].isin(df['xbrl_key'].unique())]

        to_download.reset_index(drop=True, inplace=True)
        return to_download

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
    mgr = DownloadManagerNSE(year=year, verbose=args.v)
    mgr.download(max_downloads=args.md)