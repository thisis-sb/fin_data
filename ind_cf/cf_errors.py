"""
Analyze CF errors. Currently on CA_FR data. TO DO: SHP
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import sys
import os
import glob
import traceback
import pandas as pd
from base_utils import prepare_json_key
import pygeneric.misc as pyg_misc
from pygeneric import archiver, archiver_cache

PATH_1  = os.path.join(DATA_ROOT, '02_ind_cf/02_nse_fr_archive')

''' --------------------------------------------------------------------------------------- '''
def checks_1(year):
    print('\nRunning checks_1 for year %d ...' % year)
    print('checks_1: check json_outcome & xbrl_outcome errors in meta_data files')
    df = pd.read_csv(os.path.join(PATH_1, 'metadata_%d.csv' % year))
    x = sorted(df.loc[~df['json_outcome']]['symbol'].unique())
    print('json_error: %d symbols: %s' % (len(x), ' '.join(x)))
    x = sorted(df.loc[~df['xbrl_outcome']]['symbol'].unique())
    print('xbrl_error: %d symbols: %s' % (len(x), ' '.join(x)))
    print('checks_1 Completed')
    return

def checks_2(year):
    print('\nRunning checks_2 for year %d ...' % year)
    print('checks_2: compare xbrl_size in meta_data file with len(xbrl_data) in xbrl archive')
    metadata = pd.read_csv(os.path.join(PATH_1, f'metadata_{year}.csv'))
    print('metadata.shape:', metadata.shape)

    def xbrl_archive_path_func(xbrl_link):
        _xx = metadata.loc[metadata['xbrl_link'] == xbrl_link]
        return None if _xx.shape[0] == 0 or not bool(_xx['xbrl_outcome'].values[0]) else \
            os.path.join(PATH_1, _xx['xbrl_archive_path'].values[0])

    ac_xbrl = archiver_cache.ArchiverCache(xbrl_archive_path_func, cache_size=5)

    not_matching = []
    for idx, row in metadata.iterrows():
        if bool(row['xbrl_outcome']):
            xbrl_size = row['xbrl_size']
            xbrl_data = ac_xbrl.get_value(row['xbrl_link'])
            if xbrl_data is not None and xbrl_size > 0 and xbrl_size != len(xbrl_data):
                not_matching.append({'symbol':row['symbol'],
                                     'seqNumber':row['seqNumber'],
                                     'xbrl_size': xbrl_size,
                                     'len(xbrl_data)':len(xbrl_data),
                                     'abs(size_diff)': abs(xbrl_size - len(xbrl_data))
                                     })
        if idx % 1000 == 0:
            print('  Completed %d / %d' % (idx, metadata.shape[0]))
    print('All Completed %d / %d' % (idx, metadata.shape[0]))

    if len(not_matching) > 0:
        not_matching = pd.DataFrame(not_matching)
        f1 = os.path.join(LOG_DIR, f'checks_2_{year}.csv')
        not_matching.to_csv(f1, index=False)
        print('  %d records not matching, saved in %s' % (not_matching.shape[0], f1))
        f2 = os.path.join(LOG_DIR, f'cf_errors_check2_{year}_symbols.txt')
        with open(f2, 'w') as f:
            f.write(' '.join(sorted(not_matching['symbol'].unique())))
            print('  not_matching symbols saved in', f2)
    print('checks_2 Completed')

    return

def checks_3(archive_type, clear=False):
    assert archive_type == 'json' or archive_type == 'xbrl', f'Invalid archive_type: {archive_type}'
    print('\nRunning checks_3 (ALL YEARS): archive_type: %s, clear: %s ...' % (archive_type, clear))

    archive_files = glob.glob(os.path.join(PATH_1, '20**/%s_period*' % archive_type))
    meta_data_files = glob.glob(os.path.join(PATH_1, f'metadata_*.csv'))
    key_col = 'key' if archive_type == 'json' else 'xbrl_link'
    metadata_keys = pd.concat([pd.read_csv(f) for f in meta_data_files])[key_col].unique()
    print('%d archive files, %d metadata_keys.' % (len(archive_files), len(metadata_keys)))

    print('Processing ...')
    for f in archive_files:
        ar = archiver.Archiver(f, mode='r')
        all_keys = ar.keys()
        print('   %s: size: %d' % (os.path.basename(f), len(all_keys)), end='')
        stale_keys = [k for k in all_keys if k not in metadata_keys]

        print(', %d keys appear to be stale' % len(stale_keys), end='')
        """
        ''' test & enable later since very few stale keys found so far (why not?)'''
        if clear and len(stale_keys) > 0:
            ar = archiver.Archiver(f, mode='w', update=True)
            for k in stale_keys:
                ar.remove(k)
            ar.flush()
            print(', all cleared', end='')"""
        print('.')

    print('checks_2 Completed')

    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from datetime import datetime
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-s", action='store_true', help='show all errors for the year')
    arg_parser.add_argument("-c", action='store_true', help='clear errors for the year, see j/x/sy')
    arg_parser.add_argument('-j', action='store_true', help='clear json outcome errors in the year')
    arg_parser.add_argument('-x', action='store_true', help='clear xbrl outcome errors in the year')
    arg_parser.add_argument("-sy", nargs='+', help='nse symbols')
    arg_parser.add_argument("-y", type=int, help='calendar year')

    args = arg_parser.parse_args()

    year = args.y if args.y is not None else datetime.today().year

    if args.s:
        checks_1(year)
        checks_2(year)
        checks_3(archive_type='json', clear=False)
        checks_3(archive_type='xbrl', clear=False)
    elif args.c:  # make sure it is explicitly checked
        f = os.path.join(PATH_1, 'metadata_%d.csv' % year)
        df = pd.read_csv(f)
        if not args.sy:
            errs = []
            if args.j: errs.append('json_outcome')
            if args.x: errs.append('xbrl_outcome')
            print('Clearing %s outcome errors for year %d ...' % (errs, year))
            for err in errs:
                x = df.loc[~df[err]]
                print(f'{os.path.basename(f)}: total = {df.shape[0]}, ', end='')
                print(f'{x.shape[0]} {err} errors, ', end='')
                if x.shape[0] > 0:
                    df = df[df[err]]
                    df.to_csv(f, index=False)
                    print(f'errors cleared, new_total = {df.shape[0]}')
                else:
                    print('nothing done')
                ''' should we clear archives as well?
                    Not for now. Running download_fr overwrites them'''
        else:  # args.sy is not None
            print('Clearing data for %d symbols in year %d ...' % (len(args.sy), year))
            print('  symbols:', args.sy)
            x = df.loc[df['symbol'].isin(args.sy)]
            print(f'{os.path.basename(f)}: total = {df.shape[0]}, ', end='')
            print(f'{x.shape[0]} entries for {len(args.sy)} symbols, ', end='')
            if x.shape[0] > 0:
                df = df[~df['symbol'].isin(args.sy)]
                df.to_csv(f, index=False)
                print(f'entries cleared, new_total = {df.shape[0]}')
            else:
                print('nothing done')
    else:
        arg_parser.print_help()