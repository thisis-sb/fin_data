"""
Process downloaded FR's in metadata_s1:
1. perform sanity checks
2. prepare metadata_step2
TO DO: process only delta from metadata_s1 to save time
"""
''' --------------------------------------------------------------------------------------- '''

import os
import sys
import pandas as pd
import traceback
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import base_utils
import pygeneric.archiver as pygeneric_archiver
import pygeneric.datetime_utils as pyg_du
from settings import DATA_ROOT

SUB_PATH1 = '02_ind_cf'

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    TO_PROCESS = 0
    exchange = 'nse'
    ARCHIVE_FOLDER = os.path.join(DATA_ROOT, SUB_PATH1, f'{exchange}_fr_xbrl_archive')
    METADATA_S1 = os.path.join(ARCHIVE_FOLDER, 'metadata_S1.csv')
    METADATA_S2 = (os.path.join(ARCHIVE_FOLDER, 'metadata_S2.csv'),
                   os.path.join(ARCHIVE_FOLDER, 'metadata_S2_errors.csv'))

    metadata_s1_df = pd.read_csv(METADATA_S1)
    metadata_s1_df.sort_values(by='archive_path', inplace=True)
    metadata_s1_df.reset_index(drop=True, inplace=True)

    print('metadata_s1_df, shape:', metadata_s1_df.shape)
    if TO_PROCESS != 0:
        metadata_s1_df = metadata_s1_df.tail(TO_PROCESS).reset_index(drop=True)
        print('Processing %d only.' % metadata_s1_df.shape[0])

    t1 = pyg_du.time_since_last(0)
    archive_path, archive_obj, n_errors, n_no_xbrl_data = None, None, 0, 0
    metadata_s2_df, metadata_s2_errors_df = pd.DataFrame(), pd.DataFrame()
    for idx, row in metadata_s1_df.iterrows():
        if idx > 0 and idx % 1000 == 0:
            metadata_s2_df.reset_index(drop=True, inplace=True)
            metadata_s2_df.to_csv(METADATA_S2[0], index=False)
            metadata_s2_errors_df.reset_index(drop=True, inplace=True)
            metadata_s2_errors_df.to_csv(METADATA_S2[1], index=False)
            print('metadata_s2 checkpoint done. metadata_s2_df.shape:', metadata_s2_df.shape,
                  'metadata_s2_errors_df.shape:', metadata_s2_errors_df.shape)
        if idx % 100 == 0:
            print('%d / %d done (time: %d s)' % (idx, metadata_s1_df.shape[0], pyg_du.time_since_last(0)))

        if not row['outcome'] or row['size'] <= 0:
            n_no_xbrl_data += 1
            continue

        if archive_path != row['archive_path']:
            print('  Opening:', os.path.basename(row['archive_path']))
            archive_obj = pygeneric_archiver.Archiver(row['archive_path'], mode='r')
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
    print('Final metadata_s2 saved. metadata_s2_df.shape:', metadata_s2_df.shape,
          'metadata_s2_errors_df.shape:', metadata_s2_errors_df.shape)

    print('All done. %d / %d had errors' % (n_errors, (metadata_s1_df.shape[0] - n_no_xbrl_data)))