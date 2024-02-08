"""
Download CF SHPs & create JSON/XBRL archives & metadata to access that.
More work required later - once nse_xbrl stabilizes
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import glob
from datetime import datetime
import json
import traceback
import pandas as pd
from pygeneric import archiver, http_utils
import pygeneric.misc as pyg_misc
from nse_xbrl.main_code import xbrl_parser

PATH_1 = os.path.join(DATA_ROOT, '00_common/04_nse_cf_shp')
PATH_2 = os.path.join(DATA_ROOT, '02_ind_cf/04_nse_shp_archive')

''' --------------------------------------------------------------------------------------- '''
''' Standalone Methods '''
def extract_shp_data(symbol, xbrl_data, verbose=False, run_standalone=False):
    shp_parser = xbrl_parser.XBRLCorporateFilingParser(symbol, xbrl_data, currency_unit=1)

    ''' Data dictionairy '''
    attributes = ['n_shareholders',
                  'n_fully_paid_up_shares', 'n_partly_paid_up_shares',
                  'n_underlying_outstanding_DRs',
                  'n_total_shares'
                  ]

    sh_level_1 = {'Prm': 'ShareholdingOfPromoterAndPromoterGroupI',
                  'FII': 'InstitutionsForeignI',
                  'DII': 'InstitutionsDomesticI',
                  'Gov': 'GovermentsI',
                  'EBT': 'EmployeeBenefitsTrustsI',
                  'Pub': 'NonInstitutionsI'
                  }

    sh_level_2 = {'Pub': {
        'KMP': 'KeyManagerialPersonnelI',
        'Prm_Rel': 'RelativesOfPromotersOtherThanPromoterGroupI',
        'Prm_LT': 'TrustsWhereAnyPersonBelongingToPromoterAndPromoterGroupIsisTrusteeOrBeneficiaryOrAuthorOfTrustI',
        'IEPF': 'InvestorEducationAndProtectionFundI',
        'Ret_Sml': 'ResidentIndividualShareholdersHoldingNominalShareCapitalUpToRsTwoLakhI',
        'Ret_HNI': 'ResidentIndividualShareholdersHoldingNominalShareCapitalInExcessOfRsTwoLakhI',
        'NRIs': 'NonResidentIndiansI',
        'For_Nat': 'ForeignNationalsI',
        'For_Cmp': 'ForeignCompaniesI',
        'Bod_Cor': 'BodiesCorporateI',
        'Publ_Oth': 'OtherNonInstitutionsI'
    }}

    ''' ------------------------------------------------------------------------------------ '''
    if verbose: print('Extracting basic information ...', end=' ')
    results = {'symbol': shp_parser.get('symbol', 'OneD'),
               'isin': shp_parser.get('isin', 'OneD')
               }
    try:
        results['is_psu'] = shp_parser.get('is_psu', 'OneI')
    except Exception as e:
        results['is_psu'] = None
    if verbose: print('Done')

    ''' ------------------------------------------------------------------------------------ '''
    if run_standalone:
        print('Saving data for diagnosis ...', end=' ')
        for k in attributes + ['percent_shareholding']:
            ctxs = shp_parser.get_contexts(attribute=k)
            n_shares = pd.DataFrame([{'context': ctx, 'value': shp_parser.get(k, ctx)} for ctx in ctxs])
            n_shares.to_csv(os.path.join(LOG_DIR, f'{k}.csv'), index=False)
        print('Done')

    ''' ------------------------------------------------------------------------------------ '''
    if verbose: print('Extracting group shareholding percentages ...', end=' ')
    x = {}
    for k in sh_level_1.keys():
        x[k] = shp_parser.get('percent_shareholding', sh_level_1[k])
    x['Total'] = round(sum(x.values()), 2)
    results['percent_shareholding'] = x
    if verbose: print('Done')

    ''' ------------------------------------------------------------------------------------ '''
    if verbose: print('Extracting shareholding pattern L1 ...', end=' ')
    shp_data_1 = []
    for attr in attributes:
        x = {'attribute': attr}
        for k in sh_level_1.keys():
            x[k] = shp_parser.get(attr, sh_level_1[k])
        shp_data_1.append(x)
    shp_data_1 = pd.DataFrame(shp_data_1).reset_index(drop=True)
    shp_data_1['Total'] = shp_data_1.apply(lambda x: x['Prm'] + x['FII'] + x['DII'] +
                                                     x['Gov'] + x['EBT'] + x['Pub'], axis=1)

    results['shp_data_L1'] = shp_data_1.set_index('attribute').to_json(orient='index')
    if verbose: print('Done')

    ''' ------------------------------------------------------------------------------------ '''
    if verbose: print('Extracting shareholding pattern L2 ...')
    for group in sh_level_2.keys():
        shp_data_2 = []
        for attr in attributes:
            if verbose: print('  Retrieving data for %s / %s ...' % (group, attr), end=' ')
            x = {'attribute': attr}
            l2_keys = sh_level_2[group]
            for k2 in l2_keys.keys():
                x[k2] = shp_parser.get(attr, l2_keys[k2])
            shp_data_2.append(x)
            if verbose: print('Done')

        shp_data_2 = pd.DataFrame(shp_data_2).reset_index(drop=True)
        cols_2_tot = [c for c in shp_data_2.columns if c != 'attribute']
        shp_data_2['Total'] = shp_data_2.apply(lambda x: sum([x[a] for a in cols_2_tot]), axis=1)
        results['shp_data_L2_%s' % group] = shp_data_2.set_index('attribute').to_json(orient='index')

    return results

def beautify_df(df):
    beauty = {'n_shareholders':[int],
              'n_fully_paid_up_shares':[float, 1000000],
              'n_partly_paid_up_shares':[float, 1000000],
              'n_underlying_outstanding_DRs':[float, 1000000],
              'n_total_shares':[float, 1000000]
              }
    df1 = []
    cols = [c for c in df.columns if c != 'attribute']
    for attr in df['attribute']:
        b = beauty[attr]
        v = df.loc[df['attribute'] == attr].to_dict('records')[0]
        x = {'attribute':attr}
        if b[0] == int:
            for c in cols:
                x[c] = '-' if v[c] == 0 else '%d' % v[c]
        else: # for now just float
            for c in cols:
                x[c] = '-' if v[c] == 0 else '%.2f' % (v[c] / b[1])
        df1.append(x)
    df1 = pd.DataFrame(df1).reset_index(drop=True)
    return df1

def show_shp_summary(shp_results, beautify=False, verbose=False):
    if verbose:
        print('\nShareholding Pattern L1:')
        print(120 * '-')
        df1 = pd.DataFrame.from_dict(json.loads(shp_results['shp_data_L1']), orient='index')
        df1 = df1.reset_index()
        x = beautify_df(df1.reset_index(drop=True)) if beautify else df1
        print(x.to_string(index=False))

    if verbose:
        print('\nShareholding Pattern L2 (Pub):')
        print(120 * '-')
        df2 = pd.DataFrame.from_dict(json.loads(shp_results['shp_data_L2_Pub']), orient='index')
        df2 = df2.reset_index()
        x = beautify_df(df2.reset_index(drop=True)) if beautify else df2
        print(x.to_string(index=False))

    print('\nSHP summary:')
    xj1 = json.loads(shp_results['shp_data_L1'])
    xj2 = json.loads(shp_results['shp_data_L2_Pub'])
    # print(json.dumps(xj2, indent=2))
    print('n_total_shares: %.2f cr' % (xj1['n_total_shares']['Total'] / 10e7))
    print('percent_shareholding:', shp_results['percent_shareholding'])
    print('n_shareholder: Retail Small: %7d K' % (xj2['n_shareholders']['Ret_Sml'] / 1000))
    print('n_shareholder: Retail HNI  : %7d' % xj2['n_shareholders']['Ret_HNI'])
    return

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
                           'is_psu', 'percent_shareholding', 'shp_data_size']

    def download(self, symbol=None, max_downloads=1000):
        shp_filings = pd.read_csv(os.path.join(PATH_1, 'CF_SHP_%d.csv' % self.year))
        shp_filings['date'] = pd.to_datetime(shp_filings['date'])
        shp_filings['submissionDate'] = pd.to_datetime(shp_filings['submissionDate'])
        print('\nLoaded shp_filings. Shape:', shp_filings.shape)
        if symbol is not None:
            shp_filings = shp_filings.loc[shp_filings['symbol'] == symbol].reset_index(drop=True)
            print('\nFiltered for %s, shp_filings. Shape: %s' % (symbol, shp_filings.shape))

        if shp_filings.shape[0] == 0:
            print('Nothing to download. shp_filings.shape:', shp_filings.shape)
            return

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
            'recordId': row['recordId'],
            'submissionDate': row['submissionDate'],
            'as_on_date': as_on_date,
            'is_psu': self.xbrl_metadata[xbrl_link]['is_psu'],
            'promoters': row['pr_and_prgrp'],
            'public': row['public_val'],
            'employee_trusts': row['employeeTrusts'],
            'percent_shareholding': self.xbrl_metadata[xbrl_link]['percent_shareholding'],
            'shp_data_size': self.xbrl_metadata[xbrl_link]['shp_data_size'],
            'xbrl_link': row['xbrl'],
            'xbrl_outcome':self.xbrl_metadata[xbrl_link]['xbrl_outcome'],
            'xbrl_size': self.xbrl_metadata[xbrl_link]['xbrl_size'],
            'xbrl_archive_path': self.xbrl_metadata[xbrl_link]['xbrl_archive_path'],
            'xbrl_error': self.xbrl_metadata[xbrl_link]['xbrl_error'],
            'timestamp': self.timestamp
        }

        return xbrl_outcome

    def get_xbrl_data(self, symbol, xbrl_link, as_on_date):
        if xbrl_link in self.xbrl_metadata.keys():
            return self.xbrl_metadata[xbrl_link]

        ''' valid link & not downloaded so far --> so download '''
        xbrl_outcome, xbrl_error_msg = False, 'to be filled'
        xbrl_av_dict = dict(zip(self.xbrl_attrs[4:], len(self.xbrl_attrs[4:])*[None]))
        shp_data = None
        try:
            xbrl_data = self.http_obj.http_get(xbrl_link)
            if len(xbrl_data) == 0:
                xbrl_error_msg = 'empty xbrl_data'
            else:
                shp_data = extract_shp_data(symbol, xbrl_data)
                assert list(shp_data.keys()) == ['symbol', 'isin', 'is_psu', 'percent_shareholding',
                                                 'shp_data_L1', 'shp_data_L2_Pub'],\
                    '%s / %s: %s' % (symbol, as_on_date, shp_data.keys())
                xbrl_av_dict['is_psu'] = shp_data['is_psu']
                xbrl_av_dict['percent_shareholding'] = shp_data['percent_shareholding']

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
        xbrl_archive_path = '%d/xbrl_%s' % (int(as_on_date[0:4]), symbol[0])
        if xbrl_archive_path not in self.xbrl_archives.keys():
            f = os.path.join(PATH_2, xbrl_archive_path)
            update = os.path.exists(f)
            self.xbrl_archives[xbrl_archive_path] = archiver.Archiver(f, mode='w', update=update)
        shp_data_str = json.dumps(shp_data)
        xbrl_av_dict['shp_data_size'] = len(shp_data_str)
        self.xbrl_archives[xbrl_archive_path].add(xbrl_link, shp_data_str)
        self.xbrl_metadata[xbrl_link] = {'xbrl_outcome': True, 'xbrl_size': len(xbrl_data),
                                         'xbrl_archive_path': xbrl_archive_path, 'xbrl_error': '',
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
def show_and_clear_errors(year, clear=False):
    print(f'\nRunning show_and_clear_errors({year}) ... ')
    f = os.path.join(PATH_2, f'metadata_{year}.csv')
    metadata = pd.read_csv(f)
    errors = metadata.loc[(metadata['xbrl_outcome'] == False) |
                          (metadata['xbrl_size'] == 0) |
                          (metadata['shp_data_size'] == 0)].reset_index(drop=True)
    print('metadata.shape: %s, %d error cases, symbols: %s'
          % (metadata.shape, errors.shape[0], sorted(errors['symbol'].unique())))
    if clear and errors.shape[0] > 0:
        print('Clearing the errors ...', end=' ')
        metadata = metadata.loc[~metadata['recordId'].isin(errors['recordId'])]
        metadata.to_csv(f, index=False)
        print('Done. metadata.shape:', metadata.shape)
    return


''' --------------------------------------------------------------------------------------- '''
def test_integrity():
    print('\nRunning test_integrity ... ')
    metadata = pd.concat([pd.read_csv(f) for f in glob.glob(os.path.join(PATH_2, 'metadata_*.csv'))])
    print('metadata:', metadata.shape)

    ar_dict = {}
    for ix, row in metadata.iterrows():
        if not row['xbrl_outcome']:
            continue

        if row['xbrl_archive_path'] not in ar_dict.keys():
            f = os.path.join(PATH_2, row['xbrl_archive_path'])
            ar_dict[row['xbrl_archive_path']] = archiver.Archiver(f, mode='r')

        shp_data_str = ar_dict[row['xbrl_archive_path']].get(row['xbrl_link'])
        assert len(shp_data_str) == row['shp_data_size'], \
            '%s / %s / %s: shp_data_size mismatch %d / %d' % \
            (row['symbol'], row['as_on_date'], row['recordId'], row['shp_data_size'], len(shp_data_str))

        ''' to do: more checks? '''

    print('--> test_integrity Completed, OK')
    return True

def test_symbol_shp_data(symbol, verbose=False):
    print(f'\nRunning test_symbol_shp_data for {symbol} ... ')
    metadata = pd.concat([pd.read_csv(f) for f in glob.glob(os.path.join(PATH_2, 'metadata_*.csv'))])
    metadata = metadata.loc[metadata['symbol'] == symbol].reset_index(drop=True)

    ar_dict = {}
    for ix, row in metadata.iterrows():
        print('Testing: %s / %s / %s / %s:' %
              (symbol, row['as_on_date'], row['xbrl_archive_path'], row['recordId']), end='')
        if not row['xbrl_outcome'] or row['xbrl_size'] == 0 or row['shp_data_size'] is None:
            print(' --> erroneous entry, ignored.')
        else:
            if row['xbrl_archive_path'] not in ar_dict.keys():
                f = os.path.join(PATH_2, row['xbrl_archive_path'])
                ar_dict[row['xbrl_archive_path']] = archiver.Archiver(f, mode='r')
            shp_data_str = ar_dict[row['xbrl_archive_path']].get(row['xbrl_link'])
            shp_data     = json.loads(shp_data_str)
            assert list(shp_data.keys()) == ['symbol', 'isin', 'is_psu', 'percent_shareholding',
                                                     'shp_data_L1', 'shp_data_L2_Pub'],\
                        '%s / %s: %s' % (symbol, row['as_on_date'], shp_data.keys())
            assert shp_data['symbol'] == symbol
            # TO DO: Later --> More checks & maybe display key parameters
            show_shp_summary(shp_data, beautify=False, verbose=False)
            print()

    print('--> test_symbol_shp_data Completed, OK')
    return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument('-t', action='store_true', help="Run test_shp_data")
    arg_parser.add_argument('-es', action='store_true', help="Show errors")
    arg_parser.add_argument('-ec', action='store_true', help="Clear errors")
    arg_parser.add_argument("-sy", help='NSE symbol')
    arg_parser.add_argument('-xl', help="Directly parse and Show using xbrl_link.")
    arg_parser.add_argument("-y", type=int, help='calendar year')
    arg_parser.add_argument("-md", type=int, help="max downloads")
    arg_parser.add_argument('-v', action='store_true', help="Verbose")
    args = arg_parser.parse_args()

    ''' Case 1: Test data integrity '''
    if args.t:
        test_integrity()
        test_symbol_shp_data(symbol='ASIANPAINT', verbose=args.v)
        exit(0)

    ''' Case 2: Parse shp directly from NSE using xbrl link '''
    if args.xl:
        if args.sy is None:
            arg_parser.print_help()
            print('args.sy also needed for this option')
            exit(0)
        print('\nDownloading, parsing & showing SHP for:', args.xl)
        http_obj = http_utils.HttpDownloads(max_tries=10, timeout=30)
        xbrl_data = http_obj.http_get(args.xl)
        assert len(xbrl_data) > 0, 'empty xbrl data: %s' % xbrl_link
        res = extract_shp_data('WIPRO', xbrl_data, verbose=True, run_standalone=True)
        show_shp_summary(res, beautify=False, verbose=args.v)
        exit()

    if args.y is None:
        arg_parser.print_help()
        exit(0)

    ''' Case 3: Show and/or clear error cases '''
    year = args.y
    if args.es:
        show_and_clear_errors(year, clear=args.ec)
        exit(0)

    ''' Case 4: download only for a particular NSE symbol '''
    n_downloads = 1000 if args.md is None else args.md
    if args.sy is not None:
        print('\nDownloading for symbol %s, year %s' % (args.sy, args.y))
        mgr = DownloadManagerNSE(year=year, verbose=args.v)
        mgr.download(symbol=args.sy, max_downloads=n_downloads)
    else:
        print('\nDownloading ALL year %s' % args.y)
        mgr = DownloadManagerNSE(year=year, verbose=args.v)
        mgr.download(symbol=args.sy, max_downloads=n_downloads)