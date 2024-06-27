"""
Analyze CF errors. Currently on CA_FR data.
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
    print('--> check json_outcome & xbrl_outcome errors in meta_data files')
    for f in [f'downloads_{year}.csv', f'metadata_{year}.csv']:
        df = pd.read_csv(os.path.join(PATH_1, f))
        x = sorted(df.loc[~df['json_outcome']]['symbol'].unique())
        print('  %s: json_error: %d symbols: %s' % (f, len(x), ' '.join(x)))
        x = sorted(df.loc[~df['xbrl_outcome']]['symbol'].unique())
        print('  %s: xbrl_error: %d symbols: %s' % (f, len(x), ' '.join(x)))
    print('checks_1 Completed')
    return

def checks_2(year):
    print('\nRunning checks_2 for year %d ...' % year)
    print('--> compare xbrl_size in meta_data file with len(xbrl_data) in xbrl archive')
    metadata = pd.read_csv(os.path.join(PATH_1, f'metadata_{year}.csv'))
    print('metadata.shape:', metadata.shape)

    def xbrl_archive_path_func(xbrl_link):
        _xx = metadata.loc[metadata['xbrl_link'] == xbrl_link]
        return None if _xx.shape[0] == 0 or not bool(_xx['xbrl_outcome'].values[0]) else \
            os.path.join(PATH_1, _xx['xbrl_archive_path'].values[0])

    ac_xbrl = archiver_cache.ArchiverCache(xbrl_archive_path_func, cache_size=5)

    not_matching = []
    err_list = []
    for idx, row in metadata.iterrows():
        if bool(row['xbrl_outcome']):
            xbrl_size = row['xbrl_size']
            try:
                xbrl_data = ac_xbrl.get_value(row['xbrl_link'])
            except Exception as e:
                err_list.append({'symbol':row['symbol'],
                                 'err_msg':'failed to get xbrl_data',
                                 'xbrl_link':row['xbrl_link']}
                                )
                continue
            if xbrl_data is not None and xbrl_size > 0 and xbrl_size != len(xbrl_data):
                not_matching.append({'symbol':row['symbol'],
                                     'seqNumber':row['seqNumber'],
                                     'xbrl_size': xbrl_size,
                                     'len(xbrl_data)':len(xbrl_data),
                                     'abs(size_diff)': abs(xbrl_size - len(xbrl_data))
                                     })
        if (idx+1) % 1000 == 0:
            print('  Completed %5d / %d' % ((idx+1), metadata.shape[0]))

    print('\nAll Completed %5d / %d' % ((idx+1), metadata.shape[0]))

    if len(not_matching) > 0:
        not_matching = pd.DataFrame(not_matching)
        f1 = os.path.join(LOG_DIR, f'checks_2_{year}.csv')
        not_matching.to_csv(f1, index=False)
        print('  xbrl_size not matching for %d records, saved in %s' % (not_matching.shape[0], os.path.basename(f1)))
        f2 = os.path.join(LOG_DIR, f'cf_errors_check2_{year}_symbols.txt')
        with open(f2, 'w') as f:
            f.write(' '.join(sorted(not_matching['symbol'].unique())))
            print('  not_matching symbols saved in', os.path.basename(f2))
            print('  --> to be possibly cleaned and downloaded again')

    if len(err_list) > 0:
        f3 = os.path.join(LOG_DIR, f'checks_2_ERRORS_{year}.csv')
        x = pd.DataFrame(err_list).reset_index(drop=True)
        x.to_csv(f3, index=False)
        print('  %d SEVERE ERRORS, saved in %s.\nSEVERE ERRORS in: %s'
              % (x.shape[0], os.path.basename(f3), x['symbol'].unique()))
        print('  --> to be possibly cleaned and downloaded again')

    print('checks_2 Completed')

    return

def checks_3(archive_type, clear=False):
    assert archive_type == 'json' or archive_type == 'xbrl', f'Invalid archive_type: {archive_type}'
    print('\nRunning checks_3 (all years) ...')
    print('--> check for stale keys in all %s archives, clear: %s' % (archive_type, clear))

    archive_files = glob.glob(os.path.join(PATH_1, '20**/%s_period*' % archive_type))
    meta_data_files = glob.glob(os.path.join(PATH_1, f'metadata_*.csv'))
    key_col = 'key' if archive_type == 'json' else 'xbrl_link'
    metadata_keys = pd.concat([pd.read_csv(f) for f in meta_data_files])[key_col].unique()
    print('%d archive files, %d metadata_keys.' % (len(archive_files), len(metadata_keys)))

    print('Processing ...')
    for f in archive_files:
        ar = archiver.Archiver(f, mode='r')
        all_keys = ar.keys()

        print('   %s: ' % os.path.basename(f), end='')
        stale_keys = [k for k in all_keys if k not in metadata_keys]
        print('%3d keys appear to be stale (out of %d).' % (len(stale_keys), len(all_keys)))
        """
        ''' test & enable later since very few stale keys found so far (why not?)'''
        if clear and len(stale_keys) > 0:
            ar = archiver.Archiver(f, mode='w', update=True)
            for k in stale_keys:
                ar.remove(k)
            ar.flush()
            print(', all cleared', end='')"""

    print('checks_3 Completed')

    return

def clear_errors(year, symbols=None, clear_downloads=False, clear_json=False, clear_xbrl=False,):
    files_to_clear = ['metadata_%d.csv' % year]
    if clear_downloads: files_to_clear.append('downloads_%d.csv' % year)
    if symbols is None:
        # clear json and/or xbrl errors in year from files in files_to_clear
        to_clear = []
        if clear_json: to_clear.append('json_outcome')
        if clear_xbrl: to_clear.append('xbrl_outcome')
        print('clear_errors: clearing %s errors for year %d ...' % (to_clear, year))
        for t in to_clear:
            for f in files_to_clear:
                df = pd.read_csv(os.path.join(PATH_1, f))
                x = df.loc[~df[t]]
                print(f'  {t} errors:  {os.path.basename(f)}: total = {df.shape[0]}, ', end='')
                print(f'{x.shape[0]} errors; ', end='')
                if x.shape[0] > 0:
                    df = df[df[t]]
                    df.to_csv(os.path.join(PATH_1, f), index=False)
                    print(f'cleared; new_total = {df.shape[0]}')
                else:
                    print('nothing done')
    else:
        # symbols is not None, clear all errors for symbol in year from files in files_to_clear
        print('clear_errors: for %d symbols in year %d\n  symbols: %s' % (len(symbols), year, symbols))
        for f in files_to_clear:
            df = pd.read_csv(os.path.join(PATH_1, f))
            x = df.loc[df['symbol'].isin(symbols)]
            print(f'{os.path.basename(f)}: total = {df.shape[0]}, ', end='')
            print(f'{x.shape[0]} entries for {len(symbols)} symbols, ', end='')
            if x.shape[0] > 0:
                df = df[~df['symbol'].isin(symbols)]
                df.to_csv(os.path.join(PATH_1, f), index=False)
                print(f'entries cleared, new_total = {df.shape[0]}')
            else:
                print('nothing done')
    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from datetime import datetime
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-s", action='store_true', help='show all errors for the year')
    arg_parser.add_argument("-y", type=int, help='calendar year')
    arg_parser.add_argument("-c", action='store_true', help='clear errors for the year, see j/x/sy')
    arg_parser.add_argument('-j', action='store_true', help='clear json outcome errors in the year')
    arg_parser.add_argument('-x', action='store_true', help='clear xbrl outcome errors in the year')
    arg_parser.add_argument("-sy", nargs='+', help='nse symbols')
    arg_parser.add_argument('-d', action='store_true', help='ASLO: clear downloads for the selection')

    args = arg_parser.parse_args()

    year = args.y if args.y is not None else datetime.today().year

    if args.s:
        checks_1(year)
        checks_2(year)
        checks_3(archive_type='json', clear=False)
        checks_3(archive_type='xbrl', clear=False)
    elif args.c:  # make sure it is explicitly checked
        clear_errors(year, symbols=args.sy, clear_downloads=args.d, clear_json=args.j, clear_xbrl=args.x)
    else:
        arg_parser.print_help()