"""
Download CF FRs & create JSON/XBRL archives & metadata to access that
"""
''' --------------------------------------------------------------------------------------- '''

import os
from datetime import datetime
import traceback
import pandas as pd
import pygeneric.misc as pyg_misc
import pygeneric.http_utils as pyg_http_utils
import pygeneric.archiver as pyg_archiver

PATH_1 = os.path.join(os.getenv('DATA_ROOT'), '02_ind_cf/01_nse_fr_filings')
PATH_2 = os.path.join(os.getenv('DATA_ROOT'), '02_ind_cf/02_nse_fr_archive')

''' --------------------------------------------------------------------------------------- '''
def prepare_key(row_dict):
    params = row_dict['params']
    if '&' in row_dict['symbol']:
        params = params.replace('&', '%26')
    seqNumber = row_dict['seqNumber']
    industry = row_dict['industry'] if pd.notna(row_dict['industry']) else ''
    # oldNewFlag = row_dict['oldNewFlag'] if row_dict['oldNewFlag'] is not None else ''
    oldNewFlag = row_dict['oldNewFlag'] if pd.notna(row_dict['oldNewFlag']) else ''
    reInd = row_dict['reInd']
    format_x = row_dict['format']

    key = 'params=%s&seq_id=%s' % (params, seqNumber) + \
          '&industry=%s&frOldNewFlag=%s' % (industry, oldNewFlag) + \
          '&ind=%s&format=%s' % (reInd, format_x)

    return key

''' --------------------------------------------------------------------------------------- '''
class Manager:
    def __init__(self, year):
        self.year = year
        self.json_checkpoint_step = 50
        self.json_incremental_downloads = 0
        self.session_errors = 0
        self.json_archives = {}
        self.json_metadata_dict = {}
        self.json_base_url = 'https://www.nseindia.com/api/' +\
                             'corporates-financial-results-data?index=equities&'
        self.run_timestamp = datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
        self.json_metadata_filename = os.path.join(PATH_2, '%d' % year, 'metadata_json.csv')

        fr_filings = pd.read_csv(os.path.join(PATH_1, 'CF_FR_%d.csv' % yr))
        fr_filings['filingDate'] = pd.to_datetime(fr_filings['filingDate'])
        print('\nLoaded fr_filings. Shape:', fr_filings.shape)

        if os.path.exists(self.json_metadata_filename):
            df = pd.read_csv(self.json_metadata_filename)
            df['filingDate'] = pd.to_datetime(df['filingDate'])
            self.json_metadata_dict = dict(zip(df['key'], df.to_dict('records')))
            fr_filings['key'] = fr_filings.apply(lambda x: prepare_key(x), axis=1)
            self.to_download = fr_filings.loc[~fr_filings['key'].isin(df['key'].unique())].\
                reset_index(drop=True)
        else:
            # metadata_df = pd.DataFrame()
            results_dict = {}
            self.to_download = fr_filings.copy()

        self.http_obj = pyg_http_utils.HttpDownloads(max_tries=10, timeout=10) \
            if self.to_download.shape[0] > 0 else None

    def download_all(self, max_downloads=5000):
        if self.to_download.shape[0] == 0:
            print('Nothing to download')
            return

        print('DownloadManager initialized. To download: %d' % self.to_download.shape[0])
        n_downloaded = 0
        for idx in self.to_download.index:
            pyg_misc.print_progress_str(idx + 1, self.to_download.shape[0])
            if idx % 50 == 0:
                print(' --> n_downloaded/session_errors: %d/%d'
                      % (n_downloaded, self.session_errors))
            self.download(idx)
            n_downloaded += 1
            if n_downloaded >= max_downloads:
                break
        self.flush_json()

        print('\nDownloads finished --> n_downloaded/session_errors: %d/%d'
              % (n_downloaded, self.session_errors))
        saved_metadata = pd.read_csv(self.json_metadata_filename)
        print('saved_metadata: %d rows, %d errors'
              % (saved_metadata.shape[0], saved_metadata.loc[~saved_metadata['outcome']].shape[0]))
        return

    def download(self, fr_idx):
        row = self.to_download.loc[fr_idx].to_dict()
        key = prepare_key(row)
        if key not in list(self.json_metadata_dict.keys()):
            if self.download_json(row, key):
                self.json_incremental_downloads += 1
                if self.json_incremental_downloads >= self.json_checkpoint_step:
                    self.flush_json()
        return True

    def download_json(self, row, key):
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

        archive_sub_path = '%d/json_month_%s' % (self.year, ('%d' % filingMonth).zfill(2))
        if archive_sub_path not in self.json_archives.keys():
            archive_path = os.path.join(PATH_2, archive_sub_path)
            update = os.path.exists(archive_path)
            self.json_archives[archive_sub_path] = pyg_archiver.Archiver(archive_path, mode='w',
                                                                         update=update)
        # should we check if the key exists - for earlier erroneous cases?
        self.json_archives[archive_sub_path].add(key, raw_data)

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
                                        'archive_sub_path': archive_sub_path,
                                        'error_msg': error_msg,
                                        'trace_log': trace_log,
                                        'json_url': json_url,
                                        'run_timestamp': self.run_timestamp
                                        }
        return True

    def flush_json(self):
        for k in self.json_archives.keys():
            self.json_archives[k].flush(create_parent_dir=True)
        self.json_archives.clear()
        self.json_incremental_downloads = 0
        df = pd.DataFrame(list(self.json_metadata_dict.values()))
        df.sort_values(by='filingDate', inplace=True)
        df.to_csv(self.json_metadata_filename, index=False)
        return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    yr = 2023
    mgr = Manager(yr)
    mgr.download_all()