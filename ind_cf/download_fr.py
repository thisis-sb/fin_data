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
import pygeneric.http_utils as pyg_http_utils
import pygeneric.archiver as pyg_archiver
from fin_data.ind_cf.base_utils import prepare_json_key

PATH_1 = os.path.join(DATA_ROOT, '02_ind_cf/01_nse_fr_filings')
PATH_2 = os.path.join(DATA_ROOT, '02_ind_cf/02_nse_fr_archive')

''' --------------------------------------------------------------------------------------- '''
class Manager:
    def __init__(self, year):
        self.year = year
        self.json_checkpoint_step = 100
        self.xbrl_archive_data_size = 10 * 1024 * 1024
        self.json_archives = {}
        self.json_metadata_dict = {}
        self.downloaded_xbrl_links = {}
        self.session_errors = 0
        self.json_incremental_downloads = 0
        self.http_obj = None

        self.json_base_url = 'https://www.nseindia.com/api/corporates-financial-results-data?index=equities&'
        self.run_timestamp = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        self.metadata_filename = os.path.join(PATH_2, '%d' % year, 'metadata.csv')

    def download(self, max_downloads=5000):
        fr_filings = pd.read_csv(os.path.join(PATH_1, 'CF_FR_%d.csv' % self.year))
        fr_filings['filingDate'] = pd.to_datetime(fr_filings['filingDate'])
        print('\nLoaded fr_filings. Shape:', fr_filings.shape)

        if os.path.exists(self.metadata_filename):
            df = pd.read_csv(self.metadata_filename)
            df['filingDate'] = pd.to_datetime(df['filingDate'])
            self.json_metadata_dict = dict(zip(df['key'], df.to_dict('records')))
            fr_filings['key'] = fr_filings.apply(lambda x: prepare_json_key(x), axis=1)
            self.to_download = fr_filings.loc[~fr_filings['key'].isin(df['key'].unique())]. \
                reset_index(drop=True)
            for idx, r in df.iterrows():
                self.downloaded_xbrl_links[r['xbrl']] = {'xbrl_outcome': r['xbrl_outcome'],
                                                         'xbrl_size': r['xbrl_size'],
                                                         'xbrl_archive_path':
                                                             r['xbrl_archive_path'],
                                                         'xbrl_error': r['xbrl_error']
                                                         }
        else:
            results_dict = {}
            self.to_download = fr_filings.copy()
            self.downloaded_xbrl_links = {}

        if self.to_download.shape[0] == 0:
            print('Nothing to download. metadata.shape:', pd.read_csv(self.metadata_filename).shape)
            return

        xbrl_archive_files = sorted(glob.glob(os.path.join(PATH_2, '%d/xbrl_archive_*' % self.year)))
        if len(xbrl_archive_files) == 0:
            print('No xbrl archives found.', end=' ')
            self.current_xbrl_archive_path = '%d/xbrl_archive_001' % self.year
            update = False
        else:
            self.current_xbrl_archive_path = '%d/%s' % (self.year,
                                                        xbrl_archive_files[-1].split('\\')[-1])
            update = True
        print('current_xbrl_archive_path:', self.current_xbrl_archive_path)
        self.current_xbrl_archive = pyg_archiver.Archiver(
            os.path.join(PATH_2, self.current_xbrl_archive_path), mode='w', update=update)

        print('DownloadManager initialized. To download: %d' % self.to_download.shape[0])
        self.http_obj = pyg_http_utils.HttpDownloads(max_tries=10, timeout=10)
        n_downloaded = 0
        self.session_errors = 0

        for idx in self.to_download.index:
            pyg_misc.print_progress_str(idx + 1, self.to_download.shape[0])
            if idx % self.json_checkpoint_step == 0:
                print(' --> n_downloaded/session_errors: %d/%d'
                      % (n_downloaded, self.session_errors))
            if self.download_json(idx):
                n_downloaded += 1
            if n_downloaded >= max_downloads:
                break
        self.flush_all()

        print('\nDownloads finished --> n_downloaded/session_errors: %d/%d'
              % (n_downloaded, self.session_errors))
        saved_metadata = pd.read_csv(self.metadata_filename)
        print('saved_metadata: %d rows, %d errors'
              % (saved_metadata.shape[0], saved_metadata.loc[~saved_metadata['outcome']].shape[0]))
        return

    def download_json(self, fr_idx):
        row = self.to_download.loc[fr_idx].to_dict()
        key = prepare_json_key(row)
        if key in list(self.json_metadata_dict.keys()):
            return False

        filingDate = row['filingDate']
        filingYear = int(filingDate.year)
        filingMonth = int(filingDate.month)
        assert self.year == filingYear, 'Years not matching'

        json_url = self.json_base_url + key
        try:
            json_data, raw_data = self.http_obj.http_get_both(json_url)
            assert ((json_data['seqnum']) == str(row['seqNumber']) and
                    (json_data['longname'] == row['companyName'])), \
                '[%s][%s][%s][%s]\n%s' % (json_data['seqnum'], row['seqNumber'],
                                          json_data['longname'], row['companyName'], json_url)
            outcome, resultFormat, error_msg, trace_log = True, json_data['resultFormat'], '', ''
        except Exception as e:
            outcome, error_msg, trace_log = False, 'ERROR! %s' % e, traceback.format_exc()
            json_data, resultFormat, raw_data = '', '', ''
            self.session_errors += 1

        json_archive_path = '%d/json_month_%s' % (self.year, ('%d' % filingMonth).zfill(2))
        if json_archive_path not in self.json_archives.keys():
            archive_path = os.path.join(PATH_2, json_archive_path)
            update = os.path.exists(archive_path)
            self.json_archives[json_archive_path] = pyg_archiver.Archiver(archive_path, mode='w',
                                                                         update=update)
        # should we check if the key exists - for earlier erroneous cases?
        self.json_archives[json_archive_path].add(key, raw_data)

        ''' check link & get data '''
        xbrl_meta_data = self.get_xbrl_data(row['xbrl'])

        ''' TO DO: add more relevant columns to this - for easier use later '''
        self.json_metadata_dict[key] = {'symbol': row['symbol'],
                                        'isin': row['isin'],
                                        'consolidated': row['consolidated'],
                                        'period': row['period'],
                                        'toDate': row['toDate'],
                                        'relatingTo': row['relatingTo'],
                                        'seqNumber': row['seqNumber'],
                                        'resultFormat': resultFormat,
                                        'filingDate': filingDate,
                                        'filingYear': filingYear,
                                        'filingMonth': filingMonth,
                                        'key': key,
                                        'outcome': outcome,
                                        'size': len(raw_data),
                                        'json_archive_path': json_archive_path,
                                        'error_msg': error_msg,
                                        'trace_log': trace_log,
                                        'json_url': json_url,
                                        'xbrl': row['xbrl'],
                                        'xbrl_outcome':xbrl_meta_data['xbrl_outcome'],
                                        'xbrl_size': xbrl_meta_data['xbrl_size'],
                                        'xbrl_archive_path': xbrl_meta_data['xbrl_archive_path'],
                                        'xbrl_error': xbrl_meta_data['xbrl_error'],
                                        'run_timestamp': self.run_timestamp
                                        }

        self.json_incremental_downloads += 1
        if self.json_incremental_downloads >= self.json_checkpoint_step:
            self.flush_all()

        return outcome

    def get_xbrl_data(self, xbrl_link):
        if os.path.basename(xbrl_link) == '-':
            return {'xbrl_outcome': False, 'xbrl_size': 0, 'xbrl_archive_path': None,
                    'xbrl_error': 'invalid xbrl ink: [%s]' % xbrl_link
                    }

        if xbrl_link in self.downloaded_xbrl_links.keys():
            return self.downloaded_xbrl_links[xbrl_link]

        ''' valid link & not downloaded so far --> so download '''
        try:
            xbrl_data = self.http_obj.http_get(xbrl_link)
            if (self.current_xbrl_archive.data_size() + len(xbrl_data)) > self.xbrl_archive_data_size:
                print('\n        %s size %d exceeding limit' % (self.current_xbrl_archive_path,
                                                                self.current_xbrl_archive.data_size()
                                                                + len(xbrl_data)), end='. ')
                self.current_xbrl_archive.flush(create_parent_dir=True)
                new_xan = int(self.current_xbrl_archive_path.split('_')[-1]) + 1
                self.current_xbrl_archive_path = '%d/xbrl_archive_%s'\
                                                 % (self.year, ('%d' % new_xan).zfill(3))
                self.current_xbrl_archive = pyg_archiver.Archiver(
                    os.path.join(PATH_2, self.current_xbrl_archive_path), mode='w')
                print('new current_xbrl_archive_path:', self.current_xbrl_archive_path)
            self.current_xbrl_archive.add(xbrl_link, xbrl_data)
            self.downloaded_xbrl_links[xbrl_link] = {'xbrl_outcome': True,
                                                     'xbrl_size': len(xbrl_data),
                                                     'xbrl_archive_path':
                                                         self.current_xbrl_archive_path,
                                                     'xbrl_error': ''
                                                     }
        except Exception as e:
            self.downloaded_xbrl_links[xbrl_link] = {'xbrl_outcome': False,
                                                     'xbrl_size': 0,
                                                     'xbrl_archive_path': None,
                                                     'xbrl_error': '%s\%s' % (e, traceback.format_exc())
                                                     }
        return self.downloaded_xbrl_links[xbrl_link]

    def flush_all(self):
        for k in self.json_archives.keys():
            self.json_archives[k].flush(create_parent_dir=True)
        self.json_archives.clear()
        self.json_incremental_downloads = 0

        self.current_xbrl_archive.flush(create_parent_dir=True)
        self.current_xbrl_archive = pyg_archiver.Archiver(
            os.path.join(PATH_2, self.current_xbrl_archive_path), mode='w', update=True)

        df = pd.DataFrame(list(self.json_metadata_dict.values()))
        df.sort_values(by='filingDate', inplace=True)
        df.to_csv(self.metadata_filename, index=False)
        return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    mgr = Manager(datetime.today().year if len(sys.argv) == 1 else int(sys.argv[1]))
    mgr.download()