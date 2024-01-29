"""
Download CF SHPs & create JSON/XBRL archives & metadata to access that.
More work required later - once nse_xbrl stabilizes
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
from datetime import datetime
import traceback
import pandas as pd
from pygeneric import archiver, http_utils
import pygeneric.misc as pyg_misc
from nse_xbrl.main_code import xbrl_parser

PATH_1 = os.path.join(DATA_ROOT, '00_common/04_nse_cf_shp')
PATH_2 = os.path.join(DATA_ROOT, '02_ind_cf/04_nse_shp_archive')

''' --------------------------------------------------------------------------------------- '''
class DownloadManagerNSE:
    def __init__(self, year, verbose=False):
        self.verbose = verbose
        self.year = year
        self.checkpoint_interval = 200
        self.xbrl_archives = {}
        self.full_metadata = {}
        self.xbrl_metadata = {}
        self.session_errors = 0
        self.http_obj = None

        self.timestamp = datetime.today().strftime('%Y-%m-%d-%H-%M')
        self.metadata_filename = os.path.join(PATH_2, f'metadata_{year}.csv')
        self.xbrl_attrs = ['xbrl_outcome', 'xbrl_size', 'xbrl_archive_path', 'xbrl_error',
                           'symbol', 'isin', 'is_psu',
                           'n_shareholders', 'n_shareholders_breakup', 'holding_breakup']

    def download(self, max_downloads=1000):
        shp_filings = pd.read_csv(os.path.join(PATH_1, 'CF_SHP_%d.csv' % self.year))
        shp_filings['date'] = pd.to_datetime(shp_filings['date'])
        shp_filings['submissionDate'] = pd.to_datetime(shp_filings['submissionDate'])
        print('\nLoaded shp_filings. Shape:', shp_filings.shape)

        if os.path.exists(self.metadata_filename):
            df = pd.read_csv(self.metadata_filename)
            df['submissionDate'] = pd.to_datetime(df['submissionDate'])
            self.full_metadata = dict(zip(df['xbrl_link'], df.to_dict('records')))
            self.to_download = shp_filings.loc[~shp_filings['xbrl'].isin(df['xbrl_link'].unique())]
            self.to_download.reset_index(inplace=True, drop=True)
            for idx, r in df.iterrows():
                self.xbrl_metadata[r['xbrl_link']] = dict(zip(self.xbrl_attrs, [r[c] for c in self.xbrl_attrs]))
        else:
            results_dict = {}
            self.to_download = shp_filings.copy()
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
              % (saved_metadata.shape[0], saved_metadata.loc[~saved_metadata['xbrl_outcome']].shape[0]))
        return

    def download_one(self, shp_idx):
        row = self.to_download.loc[shp_idx].to_dict()
        if row['xbrl'] in list(self.full_metadata.keys()):
            return False

        ''' check link & get data '''
        as_on_date  = row['date'].strftime('%Y-%m-%d')
        xbrl_link    = row['xbrl']
        xbrl_outcome = self.get_xbrl_data(row['symbol'], xbrl_link, as_on_date)

        self.full_metadata[xbrl_link] = {
            'symbol': row['symbol'],
            'isin': self.xbrl_metadata[xbrl_link]['isin'],
            'recordId': row['recordId'],
            'submissionDate': row['submissionDate'],
            'as_on_date': as_on_date,
            'promoters': row['pr_and_prgrp'],
            'public': row['public_val'],
            'employee_trusts': row['employeeTrusts'],
            'is_psu':self.xbrl_metadata[xbrl_link]['is_psu'],
            'n_shareholders': self.xbrl_metadata[xbrl_link]['n_shareholders'],
            'n_shareholders_breakup': self.xbrl_metadata[xbrl_link]['n_shareholders_breakup'],
            'holding_breakup': self.xbrl_metadata[xbrl_link]['holding_breakup'],
            'xbrl_link': row['xbrl'],
            'xbrl_outcome':self.xbrl_metadata[xbrl_link]['xbrl_outcome'],
            'xbrl_size': self.xbrl_metadata[xbrl_link]['xbrl_size'],
            'xbrl_archive_path': self.xbrl_metadata[xbrl_link]['xbrl_archive_path'],
            'xbrl_error': self.xbrl_metadata[xbrl_link]['xbrl_error'],
            'timestamp': self.timestamp
            # TO DO: add n_shares, n_shareholders from xbrl_data
        }

        return xbrl_outcome

    def get_xbrl_data(self, symbol, xbrl_link, as_on_date):
        if xbrl_link in self.xbrl_metadata.keys():
            return self.xbrl_metadata[xbrl_link]

        ''' valid link & not downloaded so far --> so download '''
        xbrl_outcome, xbrl_error_msg = False, 'to be filled'
        # xbrl_attrs   = self.xbrl_attrs[4:]
        xbrl_av_dict = dict(zip(self.xbrl_attrs[4:], len(self.xbrl_attrs[4:])*[None]))
        try:
            xbrl_data = self.http_obj.http_get(xbrl_link)
            if len(xbrl_data) == 0:
                xbrl_error_msg = 'empty xbrl_data'
            else:
                shp_parser = xbrl_parser.XBRLCorporateFilingParser(symbol, xbrl_data, currency_unit=1)
                xbrl_av_dict['symbol'] = shp_parser.get('symbol', 'OneD')
                xbrl_av_dict['isin'] = shp_parser.get('isin', 'OneD')
                xbrl_av_dict['is_psu'] = shp_parser.get('is_psu', 'OneI')

                x1 = shp_parser.get('n_shareholders', 'ShareholdingOfPromoterAndPromoterGroupI')
                x2 = shp_parser.get('n_shareholders', 'InstitutionsDomesticI')
                x3 = shp_parser.get('n_shareholders', 'InstitutionsForeignI')
                x4 = shp_parser.get('n_shareholders', 'GovermentsI')
                x5 = shp_parser.get('n_shareholders', 'NonInstitutionsI')
                _x = {'Prm':x1, 'DII':x2, 'FII':x3, 'Gov':x4, 'Pub':x5}
                xbrl_av_dict['n_shareholders'] = sum(_x.values())
                xbrl_av_dict['n_shareholders_breakup'] = str(_x)

                x1 = shp_parser.get('percent_shareholding', 'ShareholdingOfPromoterAndPromoterGroupI')
                x2 = shp_parser.get('percent_shareholding', 'InstitutionsDomesticI')
                x3 = shp_parser.get('percent_shareholding', 'InstitutionsForeignI')
                x4 = shp_parser.get('percent_shareholding', 'GovermentsI')
                x5 = shp_parser.get('percent_shareholding', 'NonInstitutionsI')
                _x = {'Prm': x1, 'DII': x2, 'FII': x3, 'Gov': x4, 'Pub': x5}
                _x['Total'] = round(sum(_x.values()), 2)
                xbrl_av_dict['holding_breakup'] = str(_x)

                """
                ''' For later detailed work when S fixes issues in nse_xbrl'''
                all_contexts = shp_parser.get_contexts(attribute='n_shareholders')
                all_n_shareholders = pd.DataFrame([{'key':ctx, 'value':shp_parser.get('n_shareholders', ctx)} for ctx in all_contexts])
                if symbol == 'TATACONSUM':
                    all_n_shareholders.to_csv(os.path.join(LOG_DIR, 'n_shareholders.csv'))
                xbrl_av_dict['n_shareholders'] = all_n_shareholders['value'].sum()
                """
                xbrl_outcome, xbrl_error_msg = True, ''
        except Exception as e:
            xbrl_error_msg = 'http_get or shp_parser.get failed:\n%s\n%s' % (e, traceback.format_exc())

        if not xbrl_outcome:
            self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': False, 'xbrl_size': 0,
                                             'xbrl_archive_path': None, 'xbrl_error': xbrl_error_msg,
                                             **xbrl_av_dict
                                             }
            return False

        ''' use new file structure - year/xbrl_<first_character_of_symbol>'''
        # xbrl_archive_path = '%d/xbrl_period_end_%s' % (int(as_on_date[0:4]), as_on_date[0:7])
        xbrl_archive_path = '%d/xbrl_%s' % (int(as_on_date[0:4]), symbol[0])
        ''' For now, storage in archive disabled - due to high storage '''
        if False:
            if as_on_date not in self.xbrl_archives.keys():
                f = os.path.join(PATH_2, xbrl_archive_path)
                update = os.path.exists(f)
                self.xbrl_archives[as_on_date] = archiver.Archiver(f, mode='w', update=update)
            self.xbrl_archives[as_on_date].add(xbrl_link, xbrl_data)
            self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': True, 'xbrl_size': len(xbrl_data),
                                             'xbrl_archive_path': xbrl_archive_path, 'xbrl_error': '',
                                             **xbrl_av_dict
                                             }
        else:
            self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': True, 'xbrl_size': None,
                                             'xbrl_archive_path': None, 'xbrl_error': '',
                                             **xbrl_av_dict
                                             }

        return True


    def flush_all(self):
        for k in self.xbrl_archives.keys():
            self.xbrl_archives[k].flush(create_parent_dir=True)

        df = pd.DataFrame(list(self.full_metadata.values()))
        df.sort_values(by='submissionDate', inplace=True)
        df.to_csv(self.metadata_filename, index=False)

        self.xbrl_archives.clear()

        return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-y", type=int, help='calendar year')
    arg_parser.add_argument("-md", type=int, help="max downloads")
    arg_parser.add_argument('-v', action='store_true', help="Verbose")
    args = arg_parser.parse_args()

    if args.y is None:
        arg_parser.print_help()
        exit(0)

    year = args.y
    n_downloads = 1000 if args.md is None else args.md
    print('\nArgs: year: %d, n_downloads: %d, verbose: %s' % (year, n_downloads, args.v))
    mgr = DownloadManagerNSE(year=year, verbose=args.v)
    mgr.download(max_downloads=n_downloads)
