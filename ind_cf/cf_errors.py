"""
Analyze CF errors
    1. Re-check download_fr errors and attempt to download them again
    x. More later
"""
''' --------------------------------------------------------------------------------------- '''

import sys
import os
import traceback
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import base_utils
import pygeneric.misc as pygeneric_misc
from settings import DATA_ROOT, LOG_DIR

SUB_PATH1 = '02_ind_cf'

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    exchange = 'nse'
    ARCHIVE_FOLDER = os.path.join(DATA_ROOT, SUB_PATH1, f'{exchange}_fr_xbrl_archive')
    METADATA_FILENAME = os.path.join(ARCHIVE_FOLDER, 'metadata_S1.csv')

    metadata_df = pd.read_csv(METADATA_FILENAME)
    metadata_df = metadata_df.loc[~metadata_df['outcome']].reset_index(drop=True)
    print('metadata_df: %d errors' % metadata_df.shape[0])

    fr_filings_df = base_utils.load_filings_fr(
        os.path.join(DATA_ROOT, SUB_PATH1, f'{exchange}_fr_filings/CF_FR_*.csv'))

    print('Re-checking errors ...')
    error_df = pd.DataFrame()
    for idx, row in metadata_df.iterrows():
        print(pygeneric_misc.progress_str(idx + 1, metadata_df.shape[0]), end='')
        sys.stdout.flush()
        xbrl_url = row['xbrl']

        try:
            xbrl_data = base_utils.get_xbrl(xbrl_url)
            outcome, error_msg = True, ''
        except Exception as e:
            outcome, error_msg = False, 'ERROR! %s\n%s' % (e, traceback.format_exc())
            xbrl_data = ''

        xbrl_filing_info = fr_filings_df.loc[fr_filings_df['xbrl'] == xbrl_url].reset_index(drop=True)
        n_filing_entries = len(xbrl_filing_info['symbol'].unique())
        if n_filing_entries > 1:
            print('\nOOPS: %d entries in fr_filings_df for %s' % (n_filing_entries, xbrl_url))
        error_df = pd.concat([error_df,
                              pd.DataFrame({'xbrl': xbrl_url,
                                            'size': len(xbrl_data),
                                            'outcome': outcome,
                                            'symbol':xbrl_filing_info['symbol'].values[0],
                                            'filingDate':xbrl_filing_info['filingDate'].values[0],
                                            'error_msg': error_msg
                                            }, index=[0])])
    error_df.reset_index(drop=True, inplace=True)
    error_df.to_csv(os.path.join(LOG_DIR, SUB_PATH1, 'error_df.csv'), index=False)
    print('\nDone. error_df.shape:', error_df.shape)