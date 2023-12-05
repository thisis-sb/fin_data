"""
Analyze CF errors
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

PATH_1  = os.path.join(DATA_ROOT, '02_ind_cf/01_nse_fr_filings')
PATH_2  = os.path.join(DATA_ROOT, '02_ind_cf/02_nse_fr_archive')

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from datetime import datetime
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-c", action='store_true', help='clear errors')
    arg_parser.add_argument("-y", type=int, help='calendar year')
    arg_parser.add_argument("-sy", nargs='+', help='nse symbols')
    arg_parser.add_argument('-a', action='store_true', help='clear all errors flag')
    args = arg_parser.parse_args()

    if args.c:
        if args.a:
            files = glob.glob(os.path.join(PATH_2, 'metadata_*.csv'))
            for f in files:
                df = pd.read_csv(f)
                x = df.loc[~df['json_outcome']]
                print(f'{os.path.basename(f)}: total = {df.shape[0]}, ', end='')
                print(f'json_errors: {x.shape[0]}, ', end='')
                if x.shape[0] > 0:
                    df = df[df['json_outcome']]
                    df.to_csv(f, index=False)
                    print(f'errors cleared, new_total = {df.shape[0]}')
                else:
                    print('nothing done')
        elif args.sy is not None:
            year = args.y if args.y is not None else datetime.today().year
            f = os.path.join(PATH_2, f'metadata_{year}.csv')
            df = pd.read_csv(f)
            x = df.loc[df['symbol'].isin(args.sy)]
            print(f'{os.path.basename(f)}: total = {df.shape[0]}, ', end='')
            print(f'symbol ({args.sy}): {x.shape[0]}, ', end='')
            if x.shape[0] > 0:
                df = df[~df['symbol'].isin(args.sy)]
                df.to_csv(f, index=False)
                print(f'symbol entries cleared, new_total = {df.shape[0]}')
            else:
                print('nothing done')
    else:
        print('\nShowing all errors\n')
        files = glob.glob(os.path.join(PATH_2, 'metadata_*.csv'))
        for f in files:
            df = pd.read_csv(f)
            x = df.loc[~df['json_outcome']]
            print('%s: json_error symbols:::\n%s\n' % (os.path.basename(f), x['symbol'].unique()))
            x = df.loc[~df['xbrl_outcome']]
            print('%s: xbrl_error symbols:::\n%s\n' % (os.path.basename(f), x['symbol'].unique()))
