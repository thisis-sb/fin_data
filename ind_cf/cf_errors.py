"""
Analyze CF errors
"""
''' --------------------------------------------------------------------------------------- '''

import sys
import os
import glob
import traceback
import pandas as pd
from base_utils import prepare_json_key
import pygeneric.misc as pyg_misc

PATH_1  = os.path.join(os.getenv('DATA_ROOT'), '02_ind_cf/01_nse_fr_filings')
PATH_2  = os.path.join(os.getenv('DATA_ROOT'), '02_ind_cf/02_nse_fr_archive')
LOG_DIR = LOG_DIR = os.path.join(os.getenv('LOG_ROOT'), '01_fin_data/02_ind_cf')

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    fr_filings = pd.concat(pd.read_csv(f) for f in glob.glob(os.path.join(PATH_1, 'CF_FR_2*.csv')))
    fr_filings = fr_filings.sort_values(by='toDate').reset_index(drop=True)
    fr_filings['toDate'] = pd.to_datetime(fr_filings['toDate'])
    fr_filings['key'] = fr_filings.apply(lambda x: prepare_json_key(x), axis=1)
    print('Loaded fr_filings, shape:', fr_filings.shape)

    metadata = pd.concat(pd.read_csv(f) for f in glob.glob(os.path.join(PATH_2, '**/metadata.csv')))
    print('Loaded metadata, shape:', metadata.shape)

    print('check-1: json_keys:', set(fr_filings['key'].unique()) == set(metadata['key'].unique()))
    print('check-2: xbrl:', set(fr_filings['xbrl'].unique()) == set(metadata['xbrl'].unique()))

    def save_errors(df_x, filename):
        if df_x.shape[0] > 0:
            df_x.to_csv(filename, index=False)
            print('Saved in', filename[len(os.getenv('HOME_DIR')) + 1:])
        else:
            if os.path.exists(filename):
                os.remove(filename)

    df = metadata.loc[~metadata['outcome']]
    print('check-3: %d json outcome errors.' % df.shape[0], end=' ')
    save_errors(df, os.path.join(LOG_DIR, 'json_outcome_errors.csv'))

    df = metadata.loc[~metadata['xbrl_outcome']]
    print('check-4: %d xbrl outcome errors (ex -).' % df.shape[0], end=' ')
    save_errors(df, os.path.join(LOG_DIR, 'xbrl_outcome_errors.csv'))
    print('%d unique xbrl links (across %d symbols) have errors'
          % (len(df['xbrl'].unique()), len(df['symbol'].unique())))