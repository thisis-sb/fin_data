"""
Download CF FRs & create JSON/XBRL archives & metadata to access that
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
from pygeneric import archiver, http_utils
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
        self.full_metadata = {}
        self.xbrl_metadata = {}
        self.session_errors = 0
        self.http_obj = None

        self.json_base_url = 'https://www.nseindia.com/api/corporates-financial-results-data?index=equities&'
        self.timestamp = datetime.today().strftime('%Y-%m-%d-%H-%M')
        self.metadata_filename = os.path.join(PATH_2, f'metadata_{year}.csv')

    def download(self, max_downloads=1000):
        fr_filings = pd.read_csv(os.path.join(PATH_1, 'CF_FR_%d.csv' % self.year))
        fr_filings['filingDate'] = pd.to_datetime(fr_filings['filingDate'])
        print('\nLoaded fr_filings. Shape:', fr_filings.shape)

        if os.path.exists(self.metadata_filename):
            df = pd.read_csv(self.metadata_filename)
            df['filingDate'] = pd.to_datetime(df['filingDate'])
            self.full_metadata = dict(zip(df['key'], df.to_dict('records')))
            fr_filings['key'] = fr_filings.apply(lambda x: base_utils.prepare_json_key(x), axis=1)
            self.to_download = fr_filings.loc[~fr_filings['key'].isin(df['key'].unique())]. \
                reset_index(drop=True)
            for idx, r in df.iterrows():
                self.xbrl_metadata[r['xbrl_link']] = {'xbrl_outcome': r['xbrl_outcome'],
                                                      'xbrl_size': r['xbrl_size'],
                                                      'xbrl_balance_sheet': r['xbrl_balance_sheet'],
                                                      'xbrl_archive_path': r['xbrl_archive_path'],
                                                      'xbrl_error': r['xbrl_error']
                                                      }
        else:
            results_dict = {}
            self.to_download = fr_filings.copy()
            self.xbrl_metadata = {}

        if self.to_download.shape[0] == 0:
            print('Nothing to download. metadata.shape:', pd.read_csv(self.metadata_filename).shape)
            return

        print('DownloadManagerNSE initialized. To download: %d' % self.to_download.shape[0])
        self.http_obj = http_utils.HttpDownloads(max_tries=10, timeout=30)
        n_downloaded = 0
        self.session_errors = 0

        for idx in self.to_download.index:
            pyg_misc.print_progress_str(idx + 1, self.to_download.shape[0])
            if self.download_one(idx):
                n_downloaded += 1
            if n_downloaded > 0 and n_downloaded % self.checkpoint_interval == 0:
                self.flush_all()
                print('\n--> flush_all: full_metadata size: %d, n_downloaded/session_errors: %d/%d'
                      % (len(self.full_metadata), n_downloaded, self.session_errors))
            if n_downloaded >= max_downloads:
                break
        self.flush_all()

        print('\nDownloads finished --> n_downloaded/session_errors: %d/%d'
              % (n_downloaded, self.session_errors))
        saved_metadata = pd.read_csv(self.metadata_filename)
        print('saved_metadata: %d rows, %d errors'
              % (saved_metadata.shape[0], saved_metadata.loc[~saved_metadata['json_outcome']].shape[0]))
        return

    def download_one(self, fr_idx):
        row = self.to_download.loc[fr_idx].to_dict()
        key = base_utils.prepare_json_key(row)
        if key in list(self.full_metadata.keys()):
            return False

        json_url = self.json_base_url + key
        try:
            json_data, raw_data = self.http_obj.http_get_both(json_url)
            assert ((json_data['seqnum']) == str(row['seqNumber']) and
                    (json_data['longname'] == row['companyName'])), \
                '[%s][%s][%s][%s]\n%s' % (json_data['seqnum'], row['seqNumber'],
                                          json_data['longname'], row['companyName'], json_url)
            json_outcome, resultFormat, json_error = True, json_data['resultFormat'], ''
        except Exception as e:
            json_outcome, json_data, resultFormat, raw_data  = False, '', '', ''
            json_error = 'http_get_both failed %s\n%s' % (e, traceback.format_exc())
            self.session_errors += 1

        period_end = datetime.strptime(row['toDate'], '%d-%b-%Y').strftime('%Y-%m-%d')  # archive key
        json_archive_path = '%d/json_period_end_%s' % (int(period_end[0:4]), period_end)
        if period_end not in self.json_archives.keys():
            f = os.path.join(PATH_2, json_archive_path)
            update = os.path.exists(f)
            self.json_archives[period_end] = archiver.Archiver(f, mode='w', update=update)
        self.json_archives[period_end].add(key, raw_data)

        ''' check link & get data '''
        xbrl_metadata = self.get_xbrl_data(row['xbrl'], period_end)

        ''' TO DO: add more relevant columns to this - for easier use later '''
        self.full_metadata[key] = {'symbol': row['symbol'],
                                   'isin': row['isin'],
                                   'consolidated': row['consolidated'],
                                   'period': row['period'],
                                   'toDate': row['toDate'],
                                   'relatingTo': row['relatingTo'],
                                   'seqNumber': row['seqNumber'],
                                   'resultFormat': resultFormat,
                                   'filingDate': row['filingDate'],
                                   'key': key,
                                   'json_url': json_url,
                                   'json_outcome': json_outcome,
                                   'json_size': len(raw_data),
                                   'json_archive_path': json_archive_path,
                                   'json_error': json_error,
                                   'xbrl_link': row['xbrl'],
                                   'xbrl_outcome':xbrl_metadata['xbrl_outcome'],
                                   'xbrl_balance_sheet':xbrl_metadata['xbrl_balance_sheet'],
                                   'xbrl_size': xbrl_metadata['xbrl_size'],
                                   'xbrl_archive_path': xbrl_metadata['xbrl_archive_path'],
                                   'xbrl_error': xbrl_metadata['xbrl_error'],
                                   'timestamp': self.timestamp
                                   }

        return json_outcome

    def get_xbrl_data(self, xbrl_link, fallback_period_end):
        if os.path.basename(xbrl_link) == '-':
            return {'xbrl_outcome': False,
                    'xbrl_size': 0,
                    'xbrl_balance_sheet':False,
                    'xbrl_archive_path': None,
                    'xbrl_error': 'invalid xbrl ink: [%s]' % xbrl_link
                    }

        if xbrl_link in self.xbrl_metadata.keys():
            return self.xbrl_metadata[xbrl_link]

        ''' valid link & not downloaded so far --> so download '''
        try:
            xbrl_data = self.http_obj.http_get(xbrl_link)
        except Exception as e:
            err_msg = 'http_get failed:\n%s\n%s' % (e, traceback.format_exc())
            self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': False,
                                             'xbrl_size': 0,
                                             'xbrl_balance_sheet': False,
                                             'xbrl_archive_path': None,
                                             'xbrl_error': err_msg
                                             }
            return self.xbrl_metadata[xbrl_link]

        if len(xbrl_data) == 0:
            self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': False,
                                             'xbrl_size': 0,
                                             'xbrl_balance_sheet': False,
                                             'xbrl_archive_path': None,
                                             'xbrl_error': 'empty xbrl_data'
                                             }
            return self.xbrl_metadata[xbrl_link]

        try:
            parsed_results = base_utils.parse_xbrl_fr(xbrl_data)
        except Exception as e:
            err_msg = 'parse_xbrl_fr failed (1):\n%s\n%s' % (e, traceback.format_exc())
            self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': False,
                                             'xbrl_size': 0,
                                             'xbrl_balance_sheet': False,
                                             'xbrl_archive_path': None,
                                             'xbrl_error': err_msg
                                             }
            return self.xbrl_metadata[xbrl_link]

        if not parsed_results['outcome']:
            self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': False,
                                             'xbrl_size': 0,
                                             'xbrl_balance_sheet': False,
                                             'xbrl_archive_path': None,
                                             'xbrl_error': 'parse_xbrl_fr failed (2)'
                                             }
            return self.xbrl_metadata[xbrl_link]

        ''' Later maybe do additional checks. Consciously prefer period_end from parsed_results '''
        balance_sheet = parsed_results['balance_sheet']
        period_end = parsed_results['period_end'] if parsed_results['period_end'] != 'not-found' \
            else fallback_period_end
        xbrl_archive_path = '%d/xbrl_period_end_%s' % (int(period_end[0:4]), period_end)
        if period_end not in self.xbrl_archives.keys():
            f = os.path.join(PATH_2, xbrl_archive_path)
            update = os.path.exists(f)
            self.xbrl_archives[period_end] = archiver.Archiver(f, mode='w', update=update)
        self.xbrl_archives[period_end].add(xbrl_link, xbrl_data)

        self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': True,
                                         'xbrl_size': len(xbrl_data),
                                         'xbrl_balance_sheet': balance_sheet,
                                         'xbrl_archive_path': xbrl_archive_path,
                                         'xbrl_error': ''
                                         }

        return self.xbrl_metadata[xbrl_link]

    def flush_all(self):
        for k in self.json_archives.keys():
            self.json_archives[k].flush(create_parent_dir=True)

        for k in self.xbrl_archives.keys():
            self.xbrl_archives[k].flush(create_parent_dir=True)

        df = pd.DataFrame(list(self.full_metadata.values()))
        df.sort_values(by='filingDate', inplace=True)
        df.to_csv(self.metadata_filename, index=False)

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

    print('\nArgs: src: %s, year: %s, -sy: %s, md: %d, verbose: %s' %
          (args.src, args.y, args.sy, args.md, args.v))

    year = datetime.today().year if args.y is None else int(args.y)
    n_downloads = 1000 if args.md is None else args.md
    mgr = DownloadManagerNSE(year=year, verbose=args.v)
    mgr.download(max_downloads=n_downloads)