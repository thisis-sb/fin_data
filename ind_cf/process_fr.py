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
        self.checkpoint_interval = 500

        self.final_metadata_filename = os.path.join(PATH_2, 'metadata_%d.csv' % self.year)
        self.final_metadata = {}

        self.dl_md_filename_json = os.path.join(PATH_2, f'dl_md/download_metadata_json_{year}.csv')
        self.dl_md_filename_xbrl = os.path.join(PATH_2, f'dl_md/download_metadata_xbrl_{year}.csv')
        self.dl_md_json = {}
        self.dl_md_xbrl = {}

        self.cf_fr_filename = os.path.join(PATH_1, 'CF_FR_%d.csv' % self.year)
        self.frs_to_process = None

        self.timestamp = datetime.today().strftime('%Y-%m-%d-%H-%M')

        def json_archive_path_func(json_key):
            _xx = self.dl_md_json.loc[self.dl_md_json['json_key'] == json_key, 'json_archive_path']
            return None if _xx.shape[0] == 0 else os.path.join(PATH_2, _xx.values[0])
        self.ac_json = archiver_cache.ArchiverCache(json_archive_path_func, cache_size=5)
        assert self.ac_json.all_ok(), 'ERROR! Corrupted ArchiverCache'

        def xbrl_archive_path_func(xbrl_key):
            _xx = self.dl_md_xbrl.loc[self.dl_md_xbrl['xbrl_key'] == xbrl_key, 'xbrl_archive_path']
            return None if _xx.shape[0] == 0 else os.path.join(PATH_2, _xx.values[0])
        self.ac_xbrl = archiver_cache.ArchiverCache(xbrl_archive_path_func, cache_size=5)
        assert self.ac_xbrl.all_ok(), 'ERROR! Corrupted ArchiverCache'

        return

    def process(self, max_to_process=None):
        print('\nStarting process:\n%s' % (90 * '-'))
        self.frs_to_process = self.__what_to_process__()

        if self.frs_to_process.shape[0] == 0:
            print('Nothing to process.')
            return

        if max_to_process is None: max_to_process = self.frs_to_process.shape[0]
        print('\nTo process: %d (max_to_process: %d)' % (self.frs_to_process.shape[0], max_to_process))
        t = datetime_utils.elapsed_time('ProcessCFFRs.process')
        n_processed = 0
        for idx in self.frs_to_process.index:
            misc.print_progress_str(idx + 1, self.frs_to_process.shape[0])
            row_dict = self.frs_to_process.loc[idx].to_dict()
            json_data, xbrl_data = self.__get_archive_data__(row_dict)

            result_format = 'not-found'
            if json_data is not None:
                try:
                    result_format = json_data['resultFormat']
                    # Might need to do some sanity checks here (fe, symbol, isin, etc.)
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
                    parsed_results = base_utils.parse_xbrl_data(xbrl_data)
                    xbrl_balance_sheet = parsed_results['balance_sheet']
                    # Might need to do some sanity checks here (fe, symbol, isin, etc.)
                except Exception as e:
                    row_dict['xbrl_outcome'] = False
                    row_dict['xbrl_error'] = 'parse_xbrl_data failed (1):\n%s\n%s' % (e, traceback.format_exc())

            # Not clear/TO DO/TO THINK: Use json_key or just add to list?
            self.final_metadata[row_dict['json_key']] = {
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
                'json_key': row_dict['json_key'],
                'json_outcome': row_dict['json_outcome'],
                'json_size': row_dict['json_size'],
                'json_archive_path': row_dict['json_archive_path'],
                'json_error': row_dict['json_error'],
                'xbrl_key': row_dict['xbrl_key'],
                'xbrl_outcome': row_dict['xbrl_outcome'],
                'xbrl_balance_sheet': xbrl_balance_sheet,
                'xbrl_size': row_dict['xbrl_size'],
                'xbrl_archive_path': row_dict['xbrl_archive_path'],
                'xbrl_error': row_dict['xbrl_error'],
                'processing_timestamp': self.timestamp,
                'json_link': row_dict['json_link'],
                'xbrl_link': row_dict['xbrl_link'],
            }
            n_processed += 1
            if n_processed >= max_to_process:
                break

            ''' save metadata (checkpoint) '''
            if n_processed % self.checkpoint_interval == 0:
                df = pd.DataFrame(list(self.final_metadata.values()))
                df.sort_values(by='processing_timestamp', inplace=True)
                df.to_csv(self.final_metadata_filename, index=False)
                je, xe = df.loc[~df['json_outcome']].shape[0], df.loc[df['xbrl_outcome'] != True].shape[0]
                md_file = os.path.basename(self.final_metadata_filename)
                print('\n    --> %s: %d rows, %d json errors, %d xbrl errors' % (md_file, df.shape[0], je, xe))

        ''' save metadata (final)'''
        df = pd.DataFrame(list(self.final_metadata.values()))
        df.sort_values(by='processing_timestamp', inplace=True)
        df.to_csv(self.final_metadata_filename, index=False)
        t = datetime_utils.elapsed_time('ProcessCFFRs.process')

        print('\nProcessing completed. Summary:')
        print('   n_processed: %d, metadata_%d.csv shape: %s' % (n_processed, self.year, df.shape))
        print('   time taken: %.2f seconds, %.3f seconds/record' % (t, t / n_processed))
        je, xe = df.loc[~df['json_outcome']].shape[0], df.loc[df['xbrl_outcome'] != True].shape[0]
        md_file = os.path.basename(self.final_metadata_filename)
        print('   final %s: %d rows, %d json errors, %d xbrl errors' % (md_file, df.shape[0], je, xe))
        print(90 * '-')

        return

    def __get_archive_data__(self, row_dict):
        json_data = json.loads(self.ac_json.get_value(row_dict['json_key'])) \
            if row_dict['json_outcome'] and row_dict['json_size'] > 0 else None
        xbrl_data = self.ac_xbrl.get_value(row_dict['xbrl_key']) \
            if row_dict['xbrl_outcome'] and row_dict['xbrl_size'] > 0 else None
        return json_data, xbrl_data


    def __what_to_process__(self):
        print('__what_to_process__: preparing self.frs_to_process')
        if not os.path.exists(self.cf_fr_filename):
            return pd.DataFrame()
        cf_frs = pd.read_csv(self.cf_fr_filename)
        cf_frs['filingDate'] = pd.to_datetime(cf_frs['filingDate'])
        cf_frs.sort_values(by='filingDate', inplace=True)
        print('  loaded %s, shape %s' % (os.path.basename(self.cf_fr_filename), cf_frs.shape))
        cf_frs['json_key'] = cf_frs.apply(lambda x: base_utils.prepare_json_key(x), axis=1)
        cf_frs['xbrl_key'] = cf_frs.apply(lambda x: os.path.basename(x['xbrl']), axis=1)

        self.frs_to_process = None # sanity

        ''' first skip FRs that are already processed ----------------------------------------- '''
        if os.path.exists(self.final_metadata_filename):
            df = pd.read_csv(self.final_metadata_filename)
            self.final_metadata = dict(zip(df['json_key'], df.to_dict('records')))
            print('  loaded %s, %d records' % (os.path.basename(self.final_metadata_filename), len(self.final_metadata)))
            frs2process = cf_frs.loc[~cf_frs['json_key'].isin(list(self.final_metadata.keys()))]
        else:
            frs2process = cf_frs.copy()
        print('  initial frs2process shape:', frs2process.shape)

        ''' add details of json data downloads ------------------------------------------------ '''
        assert os.path.exists(self.dl_md_filename_json),\
            '\n\n__what_to_process__: ERROR! %s does not exist' % self.dl_md_filename_json

        self.dl_md_json = pd.read_csv(self.dl_md_filename_json)
        self.dl_md_json.drop(columns='timestamp', inplace=True)
        print('  loaded %s, shape: %s' % (os.path.basename(self.dl_md_filename_json), self.dl_md_json.shape))

        ''' then consider only FRs whose json data is downloaded ------------------------------ '''
        frs2process = frs2process.loc[frs2process['json_key'].isin(self.dl_md_json['json_key'].unique())]
        # print('  (before merging): frs2process shape:', frs2process.shape)
        frs2process = frs2process.merge(self.dl_md_json, on=['symbol', 'json_key'], how='left')
        print('  (after adding json_data info): frs2process shape:', frs2process.shape)
        frs2process.to_csv(os.path.join(LOG_DIR, 'frs2process_1.csv'), index=False)
        x = frs2process['json_outcome'].isnull().sum()
        if x > 0: print('    WARNING! json_outcome null in %d rows' % x)

        ''' add details of xbrl data downloads ------------------------------------------------ '''
        assert os.path.exists(self.dl_md_filename_xbrl), \
                    '\n\n__load_files__: ERROR! %s does not exist' % self.dl_md_filename_xbrl
        self.dl_md_xbrl = pd.read_csv(self.dl_md_filename_xbrl)
        self.dl_md_xbrl.drop(columns='timestamp', inplace=True)
        print('  loaded %s, shape: %s' % (os.path.basename(self.dl_md_filename_xbrl), self.dl_md_xbrl.shape))
        frs2process = frs2process.merge(self.dl_md_xbrl, on=['symbol', 'xbrl_key'], how='left')
        print('  (after adding xbrl_data info ): frs2process shape:', frs2process.shape)
        frs2process.to_csv(os.path.join(LOG_DIR, 'frs2process_2.csv'), index=False)
        x = frs2process['xbrl_outcome'].isnull().sum()
        if x > 0: print('    WARNING! xbrl_outcome null in %d rows' % x)

        ''' continue even if xbrl_outcome is False -------------------------------------------- '''
        frs2process = frs2process.loc[frs2process['json_outcome']]
        print('final self.frs_to_process shape:', frs2process.shape)
        frs2process.to_csv(os.path.join(LOG_DIR, 'frs2process_3.csv'), index=False)

        return frs2process.reset_index(drop=True)

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

        parsed_result = base_utils.parse_xbrl_data(xbrl_data)
        [print('%s: %s' % (k, parsed_result[k])) for k in parsed_result.keys() if k != 'parsed_df']
        df = parsed_result['parsed_df']
        df.to_csv(os.path.join(LOG_DIR, 'parsed_df_%s.csv' % parsed_result['NSE Symbol']), index=False)
        print(df.loc[df['context'] == args.ctx].to_string(index=False))
    else:
        year = datetime.today().year if args.y is None else int(args.y)
        ProcessCFFRs(year=year, verbose=args.v).process(max_to_process=args.mp)