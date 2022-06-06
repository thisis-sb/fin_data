# --------------------------------------------------------------------------------------------
# Download CF FRs - and create pickle DB, meta_DB & error_DB.
# Only new files in input csv's are downloaded
# Usage: exchange
# --------------------------------------------------------------------------------------------

import sys
import os
import glob
import datetime
import time
import pandas as pd
import traceback
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import base.common
import base.archiver
import base_utils
from global_env import CONFIG_DIR, DATA_ROOT, LOG_DIR

def get_fr_xbrl_urls(exchange, redo_errors=False):
    if redo_errors:
        ERRORS_DB = base_utils.fr_meta_data_files(exchange)[1]
        assert os.path.exists(ERRORS_DB), f'{ERRORS_DB} does not exist'
        urls_df = pd.read_csv(ERRORS_DB)
    else:
        xbrl_files = glob.glob(DATA_ROOT + f'/02_ind_cf/{exchange}_fr_1/scrape_results_*.csv')
        urls_df = pd.concat([pd.read_csv(f) for f in xbrl_files])

    if exchange == 'nse':
        eql_df = pd.read_csv(CONFIG_DIR + '/EQUITY_L.csv')
        eql_df.rename(columns={'NAME OF COMPANY':'COMPANY NAME'}, inplace=True)
        urls_df = pd.merge(urls_df, eql_df, on='COMPANY NAME', how='left')
        urls_df.rename(columns={'SYMBOL':'NSE Symbol', ' ISIN NUMBER':'ISIN', ' SERIES':'SERIES',
                                '** XBRL':'XBRL Link', 'PERIOD ENDED':'PERIOD_ENDED'
                                }, inplace=True)
        urls_df = urls_df[['NSE Symbol', 'ISIN', 'SERIES',
                           'COMPANY NAME', 'PERIOD_ENDED', 'XBRL Link']]
        [urls_df[col].fillna('ZZ', inplace=True)
         for col in ['NSE Symbol', 'ISIN', 'SERIES', 'COMPANY NAME']]
    elif exchange == 'bse':
        nse_eql_df = pd.read_csv(CONFIG_DIR + '/EQUITY_L.csv')
        nse_eql_df.rename(columns={' ISIN NUMBER': 'ISIN',
                                   ' SERIES': 'SERIES',
                                   'NAME OF COMPANY':'COMPANY NAME'
                                   }, inplace=True)
        nse_eql_df = nse_eql_df[['SYMBOL', 'ISIN', 'COMPANY NAME', 'SERIES']]
        urls_df = pd.merge(urls_df, nse_eql_df, on='ISIN', how='left')
        # to do: cross-check NSE Symbol == SYMBOL
        urls_df = urls_df[['NSE Symbol', 'BSE Code', 'ISIN', 'COMPANY NAME', 'SERIES', 'XBRL Link']]
    else:
        assert False, 'Invalid Exchange'

    urls_df.reset_index(drop=True, inplace=True)
    # print(urls_df.head()); urls_df.to_csv(LOG_DIR + '/ind_cf/urls_df.csv', index=False);exit()
    return urls_df

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    exchange = 'bse' if len(sys.argv) == 1 else sys.argv[1]
    redo_errors = False  # for now, set manually

    download_timestamp = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M')
    ARCHIVE_NAME = f'{exchange}_{download_timestamp}'
    OUT_DIR = DATA_ROOT + f'/02_ind_cf/{exchange}_fr_2'
    xbrl_archive = base.archiver.Archiver(os.path.join(OUT_DIR, ARCHIVE_NAME), 'w')

    META_DATA_DB, ERRORS_DB = base_utils.fr_meta_data_files(exchange)
    if os.path.exists(META_DATA_DB):
        meta_data_df = pd.read_csv(META_DATA_DB)
        print('\nLoaded existing META_DATA_DB shape:', meta_data_df.shape)
        downloaded_fr_files = meta_data_df['XBRL_filename'].unique()
    else:
        meta_data_df = pd.DataFrame()
        downloaded_fr_files = []

    if os.path.exists(ERRORS_DB):
        errors_df = pd.read_csv(ERRORS_DB)
        print('Loaded existing ERRORS_DB shape:', errors_df.shape)
        erroneous_fr_files = errors_df['XBRL Link'].to_list()
    else:
        errors_df = pd.DataFrame()
        erroneous_fr_files = []

    xbrl_urls_df = get_fr_xbrl_urls(exchange, redo_errors)
    # xbrl_urls_df = xbrl_urls_df.head(5).reset_index(drop=True)
    # xbrl_urls_df = xbrl_urls_df[(xbrl_urls_df['NSE Symbol'].str.startswith('Z')) |
    #                            (xbrl_urls_df['NSE Symbol'].str.startswith('IC'))].reset_index()
    # print(xbrl_urls_df.shape); xbrl_urls_df.to_csv(LOG_DIR + '/urls_df.csv', index=False); exit()
    print(f'\nProcessing {xbrl_urls_df.shape[0]} files from {exchange} exchange :::')
    # exit()

    n_downloaded, n_skipped, n_errors = 0, 0, 0
    redone_errors = []
    meta_data_index, error_db_index = meta_data_df.shape[0], errors_df.shape[0]
    base.common.time_since_last(0)
    for idx, row in xbrl_urls_df.iterrows():
        verbose = False
        xbrl_url = row['XBRL Link']

        print(base.common.progress_str(idx + 1, xbrl_urls_df.shape[0]), end='')

        if (idx + 2) % 100 == 1:
            time.sleep(1)
            # later: flush intermediate results & reload

        if os.path.basename(xbrl_url) in downloaded_fr_files or \
                (not redo_errors and xbrl_url in erroneous_fr_files):
            '''print('XBRL file previously downloaded, skipping')'''
            n_skipped = n_skipped + 1
            continue

        try:
            result = base_utils.download_xbrl_fr(xbrl_url, verbose=verbose)
            assert result['outcome'], 'download_xbrl_file failed for %s' % xbrl_url

            xbrl_archive.add(os.path.basename(xbrl_url), result['xbrl_string'])

            isin = result['ISIN'] if result['ISIN'] != 'not-found' else row['ISIN']
            # fix bse later
            company_name = row['COMPANY NAME'] if exchange == 'nse' else result['company_name']

            meta_data_df = pd.concat([
                meta_data_df, pd.DataFrame(
                    {'NSE Symbol': result['NSE Symbol'],
                     'BSE Code': result['BSE Code'],
                     'ISIN': isin,
                     'SERIES':row['SERIES'],
                     'company_name': company_name,
                     'period': result['period'],
                     'result_type': result['result_type'],
                     'result_format': result['result_format'],
                     'period_start': result['period_start'],
                     'period_end': result['period_end'],
                     'quarter_type': result['quarter_type'],
                     'XBRL_archive': ARCHIVE_NAME,
                     'XBRL_filename': os.path.basename(xbrl_url),
                     'XBRL_data_size': len(result['xbrl_string']),
                     'XBRL Link': xbrl_url
                     }, index=[meta_data_index]
                )])
            meta_data_index = meta_data_index + 1
            n_downloaded = n_downloaded + 1
            if redo_errors:
                redone_errors.append(idx)
        except Exception as e:
            if not redo_errors:
                errors_df = pd.concat([
                    errors_df, pd.DataFrame(
                        {'NSE Symbol': row['NSE Symbol'],
                         'ISIN': row['ISIN'],
                         'SERIES': row['SERIES'],
                         'company_name': row['COMPANY NAME'],
                         'period_end': row['PERIOD_ENDED'],
                         'XBRL Link':xbrl_url,
                         'error_msg':e,
                         'traceback':traceback.format_exc()
                         }, index=[error_db_index])])
                error_db_index = error_db_index + 1
                n_errors = n_errors + 1
    # ------------------------------------- end of for loop ------------------------------------
    print(f'\nAll {xbrl_urls_df.shape[0]} files processed, summarizing & saving results ...')

    meta_data_df.reset_index(drop=True, inplace=True)

    if redo_errors and len(redone_errors) > 0:
        errors_df.drop(redone_errors, inplace=True)
    errors_df.reset_index(drop=True, inplace=True)

    if n_downloaded > 0:
        base.common.time_since_last(0)
        xbrl_archive.flush()
        print(f'xbrl_archive.flush() took {base.common.time_since_last(0)} seconds')

    meta_data_df.drop_duplicates(inplace=True, ignore_index=True)

    if meta_data_df.shape[0] > 0:
        meta_data_df.to_csv(META_DATA_DB, index=False)

    if errors_df.shape[0] > 0:
        errors_df.to_csv(ERRORS_DB, index=False)
    elif redo_errors and os.path.exists(ERRORS_DB):
        os.remove(ERRORS_DB)

    print()
    print(n_downloaded, 'XBRL files downloaded.')
    print(n_skipped, 'XBRL files skipped.')
    print(n_errors, 'XBRL files led to errors.')
    print(f'META_DATA_DB shape:', meta_data_df.shape)
    print(f'ERRORS_DB    shape:', errors_df.shape)

    print('ALL OK')