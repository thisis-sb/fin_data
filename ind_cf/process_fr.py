"""
Process downloaded JSON/XBRL data: analyze basic issues & create final metadata
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
from datetime import datetime
import pandas as pd
import json
import traceback
from pygeneric import archiver_cache, misc, datetime_utils
import fin_data.ind_cf.base_utils as base_utils

PATH_1 = os.path.join(DATA_ROOT, '02_ind_cf/01_nse_fr_filings')
PATH_2 = os.path.join(DATA_ROOT, '02_ind_cf/02_nse_fr_archive')

''' --------------------------------------------------------------------------------------- '''
class ProcessCFFRs:
    def __init__(self, year, verbose=False):
        self.verbose = verbose
        self.year = year
        self.checkpoint_interval = 300

        self.downloaded_data = {}
        self.final_metadata = {}
        self.frs_to_process = None

        self.timestamp = datetime.today().strftime('%Y-%m-%d-%H-%M')
        self.cf_fr_filename = os.path.join(PATH_1, 'CF_FR_%d.csv' % self.year)
        self.downloaded_data_filename = os.path.join(PATH_2, f'downloads_{self.year}.csv')
        self.final_metadata_filename = os.path.join(PATH_2, 'metadata_%d.csv' % self.year)

        def json_archive_path_func(key):
            _xx = self.downloaded_data.loc[self.downloaded_data['key'] == key, 'json_archive_path']
            return None if _xx.shape[0] == 0 else os.path.join(PATH_2, _xx.values[0])
        self.ac_json = archiver_cache.ArchiverCache(json_archive_path_func, cache_size=5)
        assert self.ac_json.all_ok(), 'ERROR! Corrupted ArchiverCache'

        def xbrl_archive_path_func(xbrl_link):
            _xx = self.downloaded_data.loc[self.downloaded_data['xbrl_link'] == xbrl_link, 'xbrl_archive_path']
            return None if _xx.shape[0] == 0 else os.path.join(PATH_2, _xx.values[0])
        self.ac_xbrl = archiver_cache.ArchiverCache(xbrl_archive_path_func, cache_size=5)
        assert self.ac_xbrl.all_ok(), 'ERROR! Corrupted ArchiverCache'

    def run(self, max_to_process=None):
        self.__load_files__()

        if self.frs_to_process.shape[0] == 0:
            print('Nothing to process.')
            return
        if max_to_process is None: max_to_process = self.frs_to_process.shape[0]

        print('\nProcessCFFRs initialized. To process: %d' % self.frs_to_process.shape[0])
        t = datetime_utils.elapsed_time('ProcessCFFRs.run')
        n_processed = 0
        for idx in self.frs_to_process.index:
            misc.print_progress_str(idx + 1, self.frs_to_process.shape[0])
            row_dict = self.frs_to_process.loc[idx].to_dict()
            json_data, xbrl_data = self.__get_archive_data__(row_dict)

            result_format = 'not-found'
            if json_data is not None:
                try:
                    result_format = json_data['resultFormat']
                except Exception as e:
                    row_dict['json_outcome'] = False
                    row_dict['json_error'] = 'corrputed json_data (%s):\n%s\n%s' % (
                        json_data, e, traceback.format_exc())
            else:
                row_dict['json_outcome'] = False
                row_dict['json_error'] = 'corrputed json_data (%s):\n%s\n%s' % (
                    json_data, e, traceback.format_exc())

            xbrl_balance_sheet = False
            if xbrl_data is not None:
                try:
                    parsed_results = base_utils.parse_xbrl_fr(xbrl_data)
                    xbrl_balance_sheet = parsed_results['balance_sheet']
                except Exception as e:
                    row_dict['xbrl_outcome'] = False
                    row_dict['xbrl_error'] = 'parse_xbrl_fr failed (1):\n%s\n%s' % (e, traceback.format_exc())

            self.final_metadata[row_dict['key']] = {
                'symbol': row_dict['symbol'],
                'isin': row_dict['isin'],
                'consolidated': row_dict['consolidated'],
                'cumulative': row_dict['cumulative'],
                'period': row_dict['period'],
                'fromDate': row_dict['fromDate'],
                'toDate': row_dict['toDate'],
                'relatingTo': row_dict['relatingTo'],
                'seqNumber': row_dict['seqNumber'],
                'resultFormat': result_format,
                'indAs':row_dict['indAs'],
                'bank': row_dict['bank'],
                'audited': row_dict['audited'],
                'oldNewFlag': row_dict['oldNewFlag'],
                'filingDate': row_dict['filingDate'],
                'key': row_dict['key'],
                'json_url': row_dict['json_url'],
                'json_outcome': row_dict['json_outcome'],
                'json_size': row_dict['json_size'],
                'json_archive_path': row_dict['json_archive_path'],
                'json_error': row_dict['json_error'],
                'xbrl_link': row_dict['xbrl_link'],
                'xbrl_outcome': row_dict['xbrl_outcome'],
                'xbrl_balance_sheet': xbrl_balance_sheet,
                'xbrl_size': row_dict['xbrl_size'],
                'xbrl_archive_path': row_dict['xbrl_archive_path'],
                'xbrl_error': row_dict['xbrl_error'],
                'processing_timestamp': self.timestamp
            }
            ''' save metadata (checkpoint) '''
            if (idx + 1) % self.checkpoint_interval == 0:
                df = pd.DataFrame(list(self.final_metadata.values()))
                df.sort_values(by='processing_timestamp', inplace=True)
                df.to_csv(self.final_metadata_filename, index=False)

            n_processed += 1
            if n_processed >= max_to_process:
                break

        ''' save metadata (final)'''
        df = pd.DataFrame(list(self.final_metadata.values()))
        df.sort_values(by='processing_timestamp', inplace=True)
        df.to_csv(self.final_metadata_filename, index=False)
        t = datetime_utils.elapsed_time('ProcessCFFRs.run')

        print('\nProcessing complete. metadata_%d.csv shape: %s' % (self.year, df.shape))
        print('time taken: %.2f seconds for %d FRs, %.3f seconds/record'
              % (t, n_processed, t / n_processed))

        return

    def __get_archive_data__(self, row_dict):
        json_data = json.loads(self.ac_json.get_value(row_dict['key'])) \
            if row_dict['json_outcome'] and row_dict['json_size'] > 0 else None
        xbrl_data = self.ac_xbrl.get_value(row_dict['xbrl_link']) \
            if row_dict['xbrl_outcome'] and row_dict['xbrl_size'] > 0 else None
        return json_data, xbrl_data


    def __load_files__(self):
        print('\nLoading files ...')
        cf_frs = pd.read_csv(self.cf_fr_filename)
        print('  loaded %s, shape %s' % (self.cf_fr_filename, cf_frs.shape))
        cf_frs['key'] = cf_frs.apply(lambda x: base_utils.prepare_json_key(x), axis=1)

        ''' first skip FRs that are already processed '''
        if os.path.exists(self.final_metadata_filename):
            df = pd.read_csv(self.final_metadata_filename)
            self.final_metadata = dict(zip(df['key'], df.to_dict('records')))
            print('  loaded %s, %d records' % (self.final_metadata_filename, len(self.final_metadata)))
            self.frs_to_process = cf_frs.loc[~cf_frs['key'].isin(list(self.final_metadata.keys()))]
        else:
            self.frs_to_process = cf_frs.copy()
        print('  initial self.frs_to_process shape:', self.frs_to_process.shape)

        ''' then consider only FRs whose data is downloaded '''
        assert os.path.exists(self.downloaded_data_filename),\
            '\n\n__load_files__() ERROR! EXITING! %s does not exist.' % self.downloaded_data_filename

        self.downloaded_data = pd.read_csv(self.downloaded_data_filename)
        print('  loaded %s, shape: %s' % (self.downloaded_data_filename, self.downloaded_data.shape))
        self.frs_to_process = self.frs_to_process.loc[
            self.frs_to_process['key'].isin(self.downloaded_data['key'].unique())]
        print('  (before merging): self.frs_to_process shape:', self.frs_to_process.shape)
        self.frs_to_process = pd.merge(self.frs_to_process, self.downloaded_data,
                                       on=['symbol', 'key'], how='left').reset_index(drop=True)
        self.frs_to_process.to_csv(os.path.join(LOG_DIR, 'df.csv'))
        print('  final (after merging): self.frs_to_process shape:', self.frs_to_process.shape)

        return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-y", help='Process for calendar year')
    arg_parser.add_argument("-mp", type=int, help='max_to_process (default all)')
    arg_parser.add_argument('-url', help="XBRL url to download & check")
    arg_parser.add_argument('-ctx', default='OneI', help="Filter for Context (only with URL)")
    arg_parser.add_argument('-v', action='store_true', help="Verbose")
    args = arg_parser.parse_args()

    if args.url is not None:
        from pygeneric.http_utils import HttpDownloads
        xbrl_data = HttpDownloads().http_get(args.url)
        assert len(xbrl_data) > 0, 'ERROR, empty XBRL data'

        parsed_result = base_utils.parse_xbrl_fr(xbrl_data)
        [print('%s: %s' % (k, parsed_result[k])) for k in parsed_result.keys() if k != 'parsed_df']
        df = parsed_result['parsed_df']
        df.to_csv(os.path.join(LOG_DIR, 'parsed_df_%s.csv' % parsed_result['NSE Symbol']), index=False)
        print(df.loc[df['context'] == args.ctx].to_string(index=False))
    else:
        year = datetime.today().year if args.y is None else int(args.y)
        ProcessCFFRs(year=year, verbose=args.v).run(max_to_process=args.mp)