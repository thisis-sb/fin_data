"""
show CF errors and delete select CF data. Currently on CA_FR data.
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

PATH_1 = os.path.join(DATA_ROOT, '02_ind_cf/01_nse_fr_filings')
PATH_2 = os.path.join(DATA_ROOT, '02_ind_cf/02_nse_fr_archive')
METADATA_FILE   = 'metadata_*.csv'
DL_MD_JSON_FILE = 'dl_md/download_metadata_json_*.csv'
DL_MD_XBRL_FILE = 'dl_md/download_metadata_xbrl_*.csv'

''' --------------------------------------------------------------------------------------- '''
def checks_1():
    print('\nchecks_1: json / xbrl outcome errors all metadata files')
    print(90*'-')

    what_to_check = {METADATA_FILE:   ['json_outcome', 'xbrl_outcome'],
                     DL_MD_JSON_FILE: ['json_outcome'],
                     DL_MD_XBRL_FILE: ['xbrl_outcome']
                     }

    for k in what_to_check.keys():
        all_files = glob.glob(os.path.join(PATH_2, k))
        for f in all_files:
            df = pd.read_csv(f)
            for c in what_to_check[k]:
                x = sorted(df.loc[df[c] != True]['symbol'].unique())
                if len(x) > 0:
                    print('  %s: %s errors in %d symbols: %s'
                          % (os.path.basename(f), c, len(x), ': %s' % ' '.join(x)))
    print(90 * '-')
    print('checks_1 Completed')

    return

def checks_2():
    print('\nchecks_2: missing metadata for CF_FR files')
    print(90 * '-')

    cf_fr_files = glob.glob(os.path.join(PATH_1, 'CF_FR_*.csv'))
    print('  checking %d CF_FR files' % len(cf_fr_files))
    je, xe = [], []
    for f in cf_fr_files:
        df1 = pd.read_csv(f)
        df1['json_key'] = df1.apply(lambda x: prepare_json_key(x), axis=1)
        df1['xbrl_key'] = df1.apply(lambda x: os.path.basename(x['xbrl']), axis=1)
        year = os.path.basename(f).split('_')[-1].split('.')[0]
        df2 = pd.read_csv(os.path.join(PATH_2, METADATA_FILE.replace('*', year)))
        x1 = df1.loc[~df1['json_key'].isin(df2['json_key'])]
        if x1.shape[0] > 0:
            df3 = pd.read_csv(os.path.join(PATH_2, DL_MD_JSON_FILE.replace('*', year)))
            x1 = x1.merge(df3, on=['symbol', 'json_key'], how='left')
            je.append(x1)
        x2 = df1.loc[~df1['xbrl_key'].isin(df2['xbrl_key'])]
        if x2.shape[0] > 0:
            df3 = pd.read_csv(os.path.join(PATH_2, DL_MD_XBRL_FILE.replace('*', year)))
            x2 = x2.merge(df3, on=['symbol', 'xbrl_key'], how='left')
            xe.append(x2)
        print('  %s: %d json_key(s) across %d symbols; %d xbrl_key(s) across %d symbols'
              % (os.path.basename(f), x1.shape[0], len(x1['symbol'].unique()),
                 x2.shape[0], len(x2['symbol'].unique())))
    if len(je) > 0:
        pd.concat([x for x in je]).to_csv(os.path.join(LOG_DIR, 'check_2_je.csv'), index=False)
    if len(xe) > 0:
        pd.concat([x for x in xe]).to_csv(os.path.join(LOG_DIR, 'check_2_xe.csv'), index=False)
    print(90 * '-')
    print('checks_2 Completed')

    return

def checks_3(archive_type, clear=False):
    assert archive_type in ['json', 'xbrl'], f'Invalid archive_type: {archive_type}'
    print('\nchecks_3: discrepancies between metadata files and archives (both directions)')
    print(90 * '-')
    key_col = 'json_key' if archive_type == 'json' else 'xbrl_key'
    print('  archive_type: %s, key_col: %s, clear flag: %s' % (archive_type, key_col, clear))

    metadata_files = glob.glob(os.path.join(PATH_2, METADATA_FILE))
    dl_md_files    = glob.glob(
        os.path.join(PATH_2, DL_MD_JSON_FILE if archive_type == 'json' else DL_MD_XBRL_FILE)
    )
    print('  %d metadata fies, %d download_metadata files' % (len(metadata_files), len(dl_md_files)))
    archive_files = glob.glob(os.path.join(PATH_2, '20**/%s_data_period*' % archive_type))
    print('  %d %s archive files' % (len(archive_files), archive_type))

    print('  --> check for %s_data with missing entries in metadata' % archive_type)
    metadata_keys = pd.concat([pd.read_csv(f) for f in metadata_files])[key_col].unique()
    dl_md_keys    = pd.concat([pd.read_csv(f) for f in dl_md_files])[key_col].unique()
    print('   %d metadata_keys, %d dl_md_keys' % (len(metadata_keys), len(dl_md_keys)))
    for f in archive_files:
        ar = archiver.Archiver(f, mode='r')
        all_keys = ar.keys()

        print('   %s: ' % os.path.basename(f), end='')
        stale_keys = [k for k in all_keys if k not in metadata_keys]
        stale_keys = [k for k in stale_keys if k not in dl_md_keys]
        if len(stale_keys) == 0:
            print(' No stale keys.')
        else:
            print('%3d keys stale (out of %d)' % (len(stale_keys), len(all_keys)), end='')
            if len(stale_keys) > 0:
                if clear:
                    ar = archiver.Archiver(f, mode='w', update=True)
                    for k in stale_keys:
                        ar.remove(k)
                    ar.flush()
                    print(', all cleared!')
                else:
                    print(', None cleared!')
            else:
                print('.')

    print('\n  --> check for metadata with missing %s_data in archives' % archive_type)
    all_archive_keys = []
    for f in archive_files:
        ar = archiver.Archiver(f, mode='r')
        [all_archive_keys.append(k) for k in ar.keys()]
    print('   %d keys in %s archives' % (len(all_archive_keys), archive_type))
    x = []
    for f in metadata_files:
        df = pd.read_csv(f)
        df['metadata_file'] = os.path.basename(f)
        x.append(df)
    df = pd.concat(x).reset_index(drop=True)
    md_keys = df[key_col].unique()
    print('   %d keys in metadata' % len(md_keys))
    missing_in_archives = [k for k in md_keys if k not in all_archive_keys]
    if len(missing_in_archives) > 0:
        print('   %d metadata keys missing in %s archives' % (len(missing_in_archives), archive_type))
        df = df.loc[df[key_col].isin(missing_in_archives)][[key_col, 'metadata_file']]
        df.to_csv(os.path.join(LOG_DIR, 'check_2_missing_in_archives.csv'), index=False)
    else:
        print('   All OK: no metadata keys missing in %s archives' % archive_type)

    print(90 * '-')
    print('checks_3 Completed')

    return

def checks_4(year):
    assert False, 'On hold since change 00119 - reactivate only if required'
    print('\nRunning checks_4 for year %d ...' % year)
    print('--> compare xbrl_size in meta_data file with len(xbrl_data) in xbrl archive')
    metadata = pd.read_csv(os.path.join(PATH_2, f'metadata_{year}.csv'))
    print('metadata.shape:', metadata.shape)

    def xbrl_archive_path_func(xbrl_link):
        _xx = metadata.loc[metadata['xbrl_key'] == xbrl_link]
        return None if _xx.shape[0] == 0 or not bool(_xx['xbrl_outcome'].values[0]) else \
            os.path.join(PATH_2, _xx['xbrl_archive_path'].values[0])
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
            if xbrl_data is not None and (xbrl_size/len(xbrl_data) < 0.95 or xbrl_size/len(xbrl_data) > 1.05):
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
    else:
        print('  NO severe errors found')

    print('checks_4 Completed')

    return

def delete_data(symbols, year):
    ''' best solution right now is to clear both json & xbrl in metadata as well as archives '''
    print('\ndelete_data: clear all data for symbols in metadata and archives')
    print(90 * '-')
    assert type(symbols) == list and type(year) == int and len(symbols) > 0
    print('  year: %d, symbols: %s' % (year, symbols))

    ''' step 1: meta data files '''
    files = [METADATA_FILE.replace('*', f'{year}'),
             DL_MD_JSON_FILE.replace('*', f'{year}'), DL_MD_XBRL_FILE.replace('*', f'{year}')]
    for f in files:
        df1 = pd.read_csv(os.path.join(PATH_2, f))
        print('  %s: shape: %s' % (f, df1.shape), end='')
        x = df1.loc[df1['symbol'].isin(symbols)]
        df1 = df1.loc[~df1['symbol'].isin(symbols)]
        df1.to_csv(os.path.join(PATH_2, f), index=False)
        print(', %d records cleared, new shape: %s' % (x.shape[0], df1.shape))

    ''' step 2: archives '''
    print('  archives: TO DO')
    print('delete_data Completed')
    print(90 * '-')
    return

def clear_errors(year, symbols=None, clear_downloads=False, clear_json=False, clear_xbrl=False):
    assert False, 'On hold since change 00119 - reactivate only if required'
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
                df = pd.read_csv(os.path.join(PATH_2, f))
                x = df.loc[~df[t]]
                print(f'  {t} errors:  {os.path.basename(f)}: total = {df.shape[0]}, ', end='')
                print(f'{x.shape[0]} errors; ', end='')
                if x.shape[0] > 0:
                    df = df[df[t]]
                    df.to_csv(os.path.join(PATH_2, f), index=False)
                    print(f'cleared; new_total = {df.shape[0]}')
                else:
                    print('nothing done')
    else:
        # symbols is not None, clear all errors for symbol in year from files in files_to_clear
        print('clear_errors: for %d symbols in year %d\n  symbols: %s' % (len(symbols), year, symbols))
        for f in files_to_clear:
            df = pd.read_csv(os.path.join(PATH_2, f))
            x = df.loc[df['symbol'].isin(symbols)]
            print(f'{os.path.basename(f)}: total = {df.shape[0]}, ', end='')
            print(f'{x.shape[0]} entries for {len(symbols)} symbols, ', end='')
            if x.shape[0] > 0:
                df = df[~df['symbol'].isin(symbols)]
                df.to_csv(os.path.join(PATH_2, f), index=False)
                print(f'entries cleared, new_total = {df.shape[0]}')
            else:
                print('nothing done')
    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from datetime import datetime
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-s", action='store_true', help='show all errors for all years')
    arg_parser.add_argument("-d", action='store_true', help='delete data for symbols in year')
    arg_parser.add_argument("-sy", nargs='+', help='nse symbols')
    arg_parser.add_argument("-y", type=int, help='calendar year')
    args = arg_parser.parse_args()

    if args.s:
        checks_1()
        checks_2()
        checks_3(archive_type='json')
        checks_3(archive_type='xbrl')
    elif args.d:
        if args.sy is None or len(args.sy) == 0 or args.y is None:
            arg_parser.print_help()
            exit()
        delete_data(symbols=args.sy, year=args.y)
    else:
        arg_parser.print_help()

    # On hold since change 00119 - reactivate only if required
    """
    arg_parser.add_argument('-j', action='store_true', help='clear json outcome errors in the year')
    arg_parser.add_argument('-x', action='store_true', help='clear xbrl outcome errors in the year')
    arg_parser.add_argument('-d', action='store_true', help='ASLO: clear downloads for the selection')
    arg_parser.add_argument("-csk", action='store_true', help='clear ALL stale keys')
    
    elif args.c:
        # On hold since change 00119 - reactivate only if required
        if not (args.j or args.x or args.sy):
            print('\nNothing done. Specify -j or -x or -sy\n')
            exit()
        clear_errors(year, symbols=args.sy, clear_downloads=args.d, clear_json=args.j, clear_xbrl=args.x)
    elif args.csk:
        checks_3(archive_type='json', clear=args.csk)
        checks_3(archive_type='xbrl', clear=args.csk)
    """