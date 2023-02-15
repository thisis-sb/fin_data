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
import base_utils
import pygeneric.misc as pyg_misc

DATA_PATH = os.path.join(os.getenv('DATA_ROOT'), '01_fin_data/02_ind_cf')
LOG_DIR   = os.path.join(os.getenv('LOG_ROOT'), '01_fin_data/02_ind_cf')

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    exchange = 'nse'
    ARCHIVE_FOLDER = os.path.join(DATA_PATH, f'{exchange}_fr_xbrl_archive')
    METADATA_FILENAME = os.path.join(ARCHIVE_FOLDER, 'metadata_S1.csv')

    metadata_df = pd.read_csv(METADATA_FILENAME)
    metadata_df = metadata_df.loc[~metadata_df['outcome']].reset_index(drop=True)
    print('metadata_df: %d errors' % metadata_df.shape[0])

    fr_filings_df = base_utils.load_filings_fr(
        os.path.join(DATA_PATH, f'{exchange}_fr_filings/CF_FR_*.csv'))

    print('Re-checking errors ...')
    successes, errors = [], []
    for idx, row in metadata_df.iterrows():
        print(pyg_misc.progress_str(idx + 1, metadata_df.shape[0]), end='')
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

        res = {'xbrl': xbrl_url,
               'size': len(xbrl_data),
               'outcome': outcome,
               'symbol':xbrl_filing_info['symbol'].values[0],
               'companyName': xbrl_filing_info['companyName'].values[0],
               'financialYear': xbrl_filing_info['financialYear'].values[0],
               'period': xbrl_filing_info['period'].values[0],
               'relatingTo': xbrl_filing_info['relatingTo'].values[0],
               'filingDate':xbrl_filing_info['filingDate'].values[0],
               'error_msg': error_msg
               }
        successes.append(res) if outcome is True else errors.append(res)
    print('\nDone')

    if len(successes) > 0:
        successes = pd.DataFrame(successes)
        successes.to_csv(os.path.join(LOG_DIR, 'successes.csv'), index=False)
        print('  %d successful, stored in successes.csv' % successes.shape[0])

    if len(errors) > 0:
        errors = pd.DataFrame(errors)
        errors.to_csv(os.path.join(LOG_DIR, 'errors.csv'), index=False)
        print('  %d failed, stored in errors.csv' % errors.shape[0])