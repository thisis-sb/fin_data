"""
Process downloaded FR's in metadata_s1:
1. perform sanity checks
2. prepare metadata_step2
"""
''' --------------------------------------------------------------------------------------- '''

import os
import sys
import pandas as pd
import traceback
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import base_utils
import pygeneric.archiver as pyg_archiver
import pygeneric.datetime_utils as pyg_dt_utils
# from settings import DATA_ROOT

# SUB_PATH1 = '02_ind_cf'
PATH_1 = os.path.join(os.getenv('DATA_ROOT'), '01_fin_data/02_ind_cf')

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    n_to_download = 5000 if len(sys.argv) != 2 else int(sys.argv[1])
    exchange = 'nse'
    ARCHIVE_FOLDER = os.path.join(PATH_1, f'{exchange}_fr_xbrl_archive')
    METADATA_S1 = os.path.join(ARCHIVE_FOLDER, 'metadata_S1.csv')
    METADATA_S2 = (os.path.join(ARCHIVE_FOLDER, 'metadata_S2.csv'),
                   os.path.join(ARCHIVE_FOLDER, 'metadata_S2_errors.csv'))

    metadata_s1_df = pd.read_csv(METADATA_S1)
    metadata_s1_df.sort_values(by='archive_path', inplace=True)
    metadata_s1_df.reset_index(drop=True, inplace=True)
    print('metadata_s1_df, shape:', metadata_s1_df.shape)

    if os.path.exists(METADATA_S2[0]):
        metadata_s2_df = pd.read_csv(METADATA_S2[0])
        print('Pre-existing %s, shape: %s' % (os.path.basename(METADATA_S2[0]), metadata_s2_df.shape))
        """
        try:
            metadata_s2_errors_df = pd.read_csv(METADATA_S2[1])
            print('Pre-existing %s, shape: %s' %
                  (os.path.basename(METADATA_S2[1]), metadata_s2_errors_df.shape))
        except pd.errors.EmptyDataError:
            metadata_s2_errors_df = pd.DataFrame()
        """
    else:
        metadata_s2_df = pd.DataFrame()
    metadata_s2_errors_df = pd.DataFrame()  # errors should always be processed

    ''' drop already processed '''
    if metadata_s2_df.shape[0] > 0:
        metadata_s1_df = metadata_s1_df.loc[~metadata_s1_df['xbrl'].isin(metadata_s2_df['xbrl'])]
    print('In all, %d UN-processed metadata_s1 rows' % metadata_s1_df.shape[0], end='. ')
    if n_to_download != 0:
        metadata_s1_df = metadata_s1_df.tail(n_to_download).reset_index(drop=True)
        print('Processing ONLY the last %d of those.' % metadata_s1_df.shape[0])
    else:
        print('Processing all of those.')

    print('\nTo process: metadata_s1_df.shape:', metadata_s1_df.shape)
    if metadata_s1_df.shape[0] == 0:
        print('Nothing to do, exit')
        exit()

    t1 = pyg_dt_utils.time_since_last(0)
    archive_path, archive_obj, n_processed, n_errors, n_no_xbrl_data = None, None, 0, 0, 0
    for idx, row in metadata_s1_df.iterrows():
        if idx > 0 and idx % 500 == 0:
            metadata_s2_df.reset_index(drop=True, inplace=True)
            metadata_s2_df.to_csv(METADATA_S2[0], index=False)
            metadata_s2_errors_df.reset_index(drop=True, inplace=True)
            metadata_s2_errors_df.to_csv(METADATA_S2[1], index=False)
            print('metadata_s2 checkpoint done. metadata_s2_df.shape:', metadata_s2_df.shape,
                  'metadata_s2_errors_df.shape:', metadata_s2_errors_df.shape)
        if idx % 100 == 0:
            print('%d / %d done (time: %d s)' % (idx, metadata_s1_df.shape[0], pyg_dt_utils.time_since_last(0)))

        if not row['outcome'] or row['size'] <= 0:
            n_no_xbrl_data += 1
            continue

        if archive_path != row['archive_path']:
            print('  Opening:', row['archive_path'].split('year_')[-1])
            archive_obj = pyg_archiver.Archiver(row['archive_path'], mode='r')
            archive_path = row['archive_path']
        try:
            parsed_result = base_utils.parse_xbrl_fr(archive_obj.get(row['xbrl']))
            metadata_s2_df = pd.concat(
                [metadata_s2_df, pd.DataFrame({'NSE Symbol':parsed_result['NSE Symbol'],
                                               'BSE Code': parsed_result['BSE Code'],
                                               'ISIN': parsed_result['ISIN'],
                                               'period_start': parsed_result['period_start'],
                                               'period_end': parsed_result['period_end'],
                                               'fy_and_qtr':parsed_result['fy_and_qtr'],
                                               'reporting_qtr': parsed_result['reporting_qtr'],
                                               'result_type': parsed_result['result_type'],
                                               'result_format': parsed_result['result_format'],
                                               'audited': parsed_result['audited'],
                                               'balance_sheet': parsed_result['balance_sheet'],
                                               'company_name': parsed_result['company_name'],
                                               'xbrl': row['xbrl'],
                                               'outcome':parsed_result['outcome'],
                                               'filingDate':row['filingDate']
                                               }, index=[idx])])
            n_processed += 1
        except Exception as e:
            n_errors += 1
            metadata_s2_errors_df = pd.concat(
                [metadata_s2_errors_df,
                 pd.DataFrame({'xbrl':row['xbrl'],
                               'error_msg':e,
                               'traceback':traceback.format_exc()
                               }, index=[idx])])

    metadata_s2_df.reset_index(drop=True, inplace=True)
    metadata_s2_df.to_csv(METADATA_S2[0], index=False)
    metadata_s2_errors_df.reset_index(drop=True, inplace=True)
    metadata_s2_errors_df.to_csv(METADATA_S2[1], index=False)
    print('Processing finished.')

    print('\nFinal metadata_s2 saved. metadata_s2_df.shape:', metadata_s2_df.shape,
          'metadata_s2_errors_df.shape:', metadata_s2_errors_df.shape)
    print('Summary: %d success, %d errors, %d with no xbrl data (crosscheck: %s)'
          % (n_processed, n_errors, n_no_xbrl_data,
             (n_processed + n_errors + n_no_xbrl_data) == metadata_s1_df.shape[0]))