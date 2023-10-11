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
from fin_data.ind_cf import scrape_bse

PATH_1 = os.path.join(DATA_ROOT, '02_ind_cf/01_nse_fr_filings')
PATH_2 = os.path.join(DATA_ROOT, '02_ind_cf/02_nse_fr_archive')
PATH_3 = os.path.join(DATA_ROOT, '02_ind_cf/03_bse_fr_archive')

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
class DownloadManagerBSE:
    def __init__(self, verbose=False):
        self.verbose=verbose
        self.bse_scrapper = None
        self.http_obj = None
        """nse_metadata_files = glob.glob(os.path.join(PATH_2, f'**/metadata.csv'))
        x = pd.concat([pd.read_csv(f) for f in nse_metadata_files])
        self.nse_meta_data = x[~x['xbrl_outcome']]
        if self.verbose:
            print('self.nse_meta_data.shape:', self.nse_meta_data.shape)"""
        self.xbrl_archives = {}
        self.bse_meta_data = {}

        bse_meta_data_file = os.path.join(PATH_3, 'metadata.csv')
        if os.path.exists(bse_meta_data_file):
            df = pd.read_csv(os.path.join(PATH_3, 'metadata.csv'))
            print('Loaded existing bse_meta_data. Shape:', df.shape)
            self.bse_meta_data = dict(zip(df['xbrl_link'], df.to_dict('records')))
            assert df.shape[0] == len(self.bse_meta_data)

    def download(self, symbols):
        assert type(symbols) == list, f'Invalid symbols type passed {type(symbols)}'
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

        self.bse_scrapper = scrape_bse.ScrapeBSE(verbose=False)
        self.http_obj = http_utils.HttpDownloads(website='bse')

        n_symbols, n_downloded, error_list = 0, 0, []
        self.download_stats = {}
        for symbol in symbols:
            print('\nDownloading for %s (%d / %d) ...' % (symbol, n_symbols + 1, len(symbols)))
            outcome, xbrl_links, err_msg = self.bse_scrapper.scrape_fr(symbol)
            if not outcome:
                error_list.append({'symbol': symbol,
                                   'xbrl_link': None,
                                   'error_msg': 'ScrapeBSE.scrape_fr failed\n%s' % err_msg
                                   })
            print('ScrapeBSE.scrape_fr outcome: %s / %d xbrl_links' % (outcome, len(xbrl_links)))
            self.download_stats[symbol] = {'symbol':symbol,
                                           'total_xbrl_links':len(xbrl_links),
                                           'downloaded_xbrl_links':0,
                                           'pre_existing':0,
                                           'erroneous':0
                                           }

            for xbrl_link in xbrl_links:
                ''' check in meta data if already downloaded '''
                if xbrl_link in self.bse_meta_data.keys():
                    self.download_stats[symbol]['pre_existing'] += 1
                    continue

                if self.verbose:
                    print('Downloading', xbrl_link)

                try:
                    xbrl_data = self.http_obj.http_get(xbrl_link)
                except Exception as e:
                    error_list.append({'symbol': symbol,
                                       'xbrl_link': xbrl_link,
                                       'error_msg': 'http_get: %s\n%s' % (e, traceback.format_exc())
                                       })
                    self.download_stats[symbol]['erroneous'] += 1
                    continue
                if len(xbrl_data) == 0:
                    error_list.append({'symbol': symbol,
                                       'xbrl_link': xbrl_link,
                                       'error_msg':'http_get failed or empty xbrl_data'
                                       })
                    self.download_stats[symbol]['erroneous'] += 1
                    continue

                try:
                    parsed_result = base_utils.parse_xbrl_fr(xbrl_data)
                except Exception as e:
                    parsed_result = {'outcome': False}  # for now quick fix

                if not parsed_result['outcome']:
                    error_list.append({'symbol': symbol,
                                       'xbrl_link': xbrl_link,
                                       'error_msg': 'corrputed xbrl_data'
                                       })
                    self.download_stats[symbol]['erroneous'] += 1
                    continue

                if self.verbose:
                    [print('%s: %s' % (k, parsed_result[k]))
                     for k in parsed_result.keys() if k != 'parsed_df']
                period_end = parsed_result['period_end']
                year = int(period_end[0:4])
                archiver_file_name = os.path.join(PATH_3,
                                                  '%d/xbrl_period_end_%s' % (year, period_end))
                if period_end not in self.xbrl_archives.keys():
                    ''' update = True --> load existing first '''
                    self.xbrl_archives[period_end] = archiver.Archiver(
                        archiver_file_name, mode='w', update=os.path.exists(archiver_file_name))
                self.xbrl_archives[period_end].add(xbrl_link, xbrl_data)
                new_row = {'symbol':symbol,
                           'bse_code': parsed_result['BSE Code'],
                           'isin':parsed_result['ISIN'],
                           'period_start':parsed_result['period_start'],
                           'period_end': period_end,
                           'fy_and_qtr': parsed_result['fy_and_qtr'],
                           'reporting_qtr': parsed_result['reporting_qtr'],
                           'result_type':parsed_result['result_type'],
                           'result_format': parsed_result['result_format'],
                           'audited': parsed_result['audited'],
                           'xbrl_balance_sheet': parsed_result['balance_sheet'],
                           'company_name': parsed_result['company_name'],
                           'xbrl_link': xbrl_link,
                           'xbrl_archive_path':'%d/xbrl_period_end_%s' % (year, period_end),
                           'timestamp':timestamp
                           }
                self.bse_meta_data[xbrl_link] = new_row
                n_downloded += 1
                self.download_stats[symbol]['downloaded_xbrl_links'] += 1
                if n_downloded % 100 == 0:
                    _, sh = self.flush_all()
                    print('  %d downloaded (%d / %d symbols) & flushed. bse_meta_data.shape: %s'
                          % (n_downloded, n_symbols, len(symbols), sh))
            n_symbols += 1

        ts = timestamp.replace(':', '-')
        os.makedirs(os.path.join(PATH_3, 'download_logs'), exist_ok=True)

        if n_downloded > 0:
            _, sh = self.flush_all()
            print('\nDownloadManagerBSE.download:', end=' ')
            print('%d downloaded (%d / %d symbols) & flushed. bse_meta_data.shape: %s'
                  % (n_downloded, n_symbols, len(symbols), sh))
            x = pd.DataFrame(self.download_stats.values())
            x.to_csv(os.path.join(PATH_3, f'download_logs/download_stats_{ts}.csv'), index=False)
            print('\nself.download_stats:\n%s' % x.to_string(index=False))
        else:
            print('\n!!! Nothing downloaded !!!')

        if len(error_list) > 0:
            x = pd.DataFrame(error_list)
            x.to_csv(os.path.join(PATH_3, f'download_logs/download_errors_{ts}.csv'), index=False)
            print('\n\n%d errors in download_errors.csv' % x.shape[0])

        return True

    def flush_all(self):
        if len(self.bse_meta_data) > 0:
            for k in self.xbrl_archives.keys():
                self.xbrl_archives[k].flush(create_parent_dir=True)
            df = pd.DataFrame(self.bse_meta_data.values())
            df.to_csv(os.path.join(PATH_3, 'metadata.csv'), index=False)
            self.xbrl_archives.clear()
            return True, df.shape
        return True, pd.DataFrame().shape

    def purge_symbols(self):
        print('TO DO')

    def sanity_check(self):
        print('TO DO')

    def download_all(self, year=None):
        files = glob.glob(os.path.join(PATH_2, 'metadata_*.csv')) if year is None else \
            [os.path.join(PATH_2, f'metadata_{year}.csv')]
        df = pd.concat([pd.read_csv(f) for f in files]) if len(files) > 1 else pd.read_csv(files[0])
        assert df.shape[0] > 0, 'Empty meta_data. File(s) used:\n%s'

        xbrl_errors = df[~df['xbrl_outcome']]
        print('xbrl_errors: shape: %s; %d unique symbols'
              % (xbrl_errors.shape, len(xbrl_errors['symbol'].unique())))
        self.download(symbols=sorted(list(xbrl_errors['symbol'].unique())))

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-src", help='Source: nse OR bse OR bse-all')
    arg_parser.add_argument("-y", help='calendar year')
    arg_parser.add_argument("-sy", nargs='+', help="list of nse symbols")
    arg_parser.add_argument("-md", type=int, default=1000, help="max downloads")
    arg_parser.add_argument('-v', action='store_true', help="Verbose")
    args = arg_parser.parse_args()

    print('\nArgs: src: %s, year: %s, -sy: %s, md: %d, verbose: %s' %
          (args.src, args.y, args.sy, args.md, args.v))

    if args.src == 'nse':
        year = datetime.today().year if args.y is None else int(args.y)
        n_downloads = 1000 if args.md is None else args.md
        mgr = DownloadManagerNSE(year=year, verbose=args.v)
        mgr.download(max_downloads=n_downloads)
    elif args.src == 'bse':
        symbols = args.sy
        DownloadManagerBSE(verbose=args.v).download(symbols)
    elif args.src == 'bse-all':
        year = args.y if args.y is None else int(args.y)
        DownloadManagerBSE(verbose=args.v).download_all(year=year)
    else:
        print(arg_parser.print_help())