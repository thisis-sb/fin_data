"""
Download CF FRs & create XBRL archive & metadata to access that
Usage: n_to_download exchange
Open Issues / TO DO:
    1. What to do with errors later on (if they're fixed)
"""
''' --------------------------------------------------------------------------------------- '''

import sys
import os
import glob
import datetime
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pygeneric.archiver as pyg_archiver
import pygeneric.misc as pyg_misc
import base_utils
from settings import DATA_ROOT

SUB_PATH1 = '02_ind_cf'
CONFIG_SYM = '02_nse_symbols'

''' --------------------------------------------------------------------------------------- '''
def current_asn(xp):
    al = glob.glob('%s/**/**/archive_*' % xp)
    assert len(al) > 0, f'{xp}: Something went really bad. len(al): {len(al)}'
    current_archive_path = sorted(al)[-1]
    return int(os.path.basename(current_archive_path).split('_')[1])

def get_asf(asn, divider):
    """ Logic: divmod(asn, divider=ARCHIVES_PER_SUB_FOLDER) """
    return ('%d' % divmod(asn, divider)[0]).zfill(2)

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    n_to_download = 5000 if len(sys.argv) != 2 else int(sys.argv[1])
    exchange = 'nse' if len(sys.argv) != 3 else sys.argv[2]

    fr_filings_df = base_utils.load_filings_fr(
        os.path.join(DATA_ROOT, SUB_PATH1, f'{exchange}_fr_filings/CF_FR_*.csv'))
    fr_filings_df['filingDate'] = pd.to_datetime(fr_filings_df['filingDate'])
    fr_filings_df.sort_values(by='filingDate')
    fr_filings_df.reset_index(drop=True, inplace=True)

    ARCHIVE_FOLDER = os.path.join(DATA_ROOT, SUB_PATH1, f'{exchange}_fr_xbrl_archive')
    METADATA_FILENAME = os.path.join(ARCHIVE_FOLDER, 'metadata_S1.csv')

    ''' Important:
    50 archives per sub_folder. 50 xbrl files per archive.
    With 20 sub_folders, can store 100000 xbrl files --> Ok for 3 more years (across 40 sub_folders)
    '''
    ARCHIVES_PER_SUB_FOLDER = 50
    ARCHIVE_MAX_SIZE = 50

    if os.path.exists(METADATA_FILENAME):
        metadata_df = pd.read_csv(METADATA_FILENAME)
        df = fr_filings_df.loc[~fr_filings_df['xbrl'].isin(metadata_df['xbrl'].unique())]
        xbrl_links_to_download = df['xbrl'].unique()
        archive_sequence_number = current_asn(ARCHIVE_FOLDER)
    else:
        metadata_df = pd.DataFrame()
        xbrl_links_to_download = fr_filings_df['xbrl'].unique()
        archive_sequence_number = 0
    print('archive_sequence_number:', archive_sequence_number)
    print('metadata_df.shape:', metadata_df.shape)
    print('In all, %d UN-downloaded xbrl links' % len(xbrl_links_to_download))

    print('\nXBRL downloads START ...')
    run_timestamp = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M-%S')
    xbrl_archive, archive_path, n_downloaded, n_errors = None, None, 0, 0
    for idx, xbrl_url in enumerate(xbrl_links_to_download):
        pyg_misc.print_progress_str(idx + 1, len(xbrl_links_to_download))

        if os.path.basename(xbrl_url) == '-' or xbrl_url not in xbrl_links_to_download:
            assert False, f'Something went wrong! {xbrl_url}'

        filing_date = fr_filings_df.loc[fr_filings_df['xbrl'] == xbrl_url, 'filingDate'].values[-1]
        fileName    = fr_filings_df.loc[fr_filings_df['xbrl'] == xbrl_url, 'fileName'].values[-1]
        filingYear  = 'year_%s' % fileName.split('_')[-1].split('.')[0]

        try:
            xbrl_data = base_utils.get_xbrl(xbrl_url)
            outcome, error_msg = True, ''
        except Exception as e:
            outcome, error_msg = False, 'ERROR! %s' % e
            xbrl_data = ''
            n_errors += 1

        if xbrl_archive is None:
            """
            1. check if archive with current sequence number exists.
            2. if yes, open it and check if it has space. If yes, continue to use it.
            3. if not, increase sequence number and create a new archive
            """
            archive_name = 'archive_%s' % ('%d' % archive_sequence_number).zfill(3)
            archive_sub_folder = get_asf(archive_sequence_number, divider=ARCHIVES_PER_SUB_FOLDER)

            archive_path = os.path.join(ARCHIVE_FOLDER, '%s/%s/%s'
                                        % (filingYear, archive_sub_folder, archive_name))

            if os.path.exists(archive_path) and pyg_archiver.\
                    Archiver(archive_path, mode='r').size() < ARCHIVE_MAX_SIZE:
                update = True
            else:
                if os.path.exists(archive_path) and pyg_archiver.\
                        Archiver(archive_path, mode='r').size() == ARCHIVE_MAX_SIZE:
                    archive_sequence_number += 1  # messy for now
                archive_name = 'archive_%s' % ('%d' % archive_sequence_number).zfill(3)
                archive_sub_folder = get_asf(archive_sequence_number, divider=ARCHIVES_PER_SUB_FOLDER)
                archive_path = os.path.join(ARCHIVE_FOLDER, '%s/%s/%s'
                                            % (filingYear, archive_sub_folder, archive_name))
                update = False
            xbrl_archive = pyg_archiver.Archiver(archive_path, mode='w', update=update)

        xbrl_archive.add(xbrl_url, xbrl_data)
        metadata_df = pd.concat(
            [metadata_df, pd.DataFrame({'xbrl':xbrl_url,
                                        'filingYear':filingYear,
                                        'filingDate':filing_date,
                                        'archive_path': archive_path,
                                        'size': len(xbrl_data),
                                        'outcome':outcome,
                                        'error_msg':error_msg,
                                        'run_timestamp':run_timestamp
                                        }, index=[0])])

        if xbrl_archive.size() >= ARCHIVE_MAX_SIZE:
            ''' perform checkpoint '''
            filingYearDir = os.path.dirname(os.path.dirname(archive_path))
            if not os.path.exists(filingYearDir):
                os.mkdir(filingYearDir)
            xbrl_archive.flush(create_parent_dir=True)
            metadata_df['filingDate'] = pd.to_datetime(metadata_df['filingDate'])
            metadata_df.sort_values(by='filingDate', inplace=True)
            metadata_df.to_csv(METADATA_FILENAME, index=False)
            xbrl_archive, archive_path = None, None  # make sure
            archive_sequence_number += 1  # see messy above

        n_downloaded += 1
        if n_downloaded >= n_to_download:
            break

    if xbrl_archive is not None and n_downloaded > 0:
        ''' last flush '''
        filingYearDir = os.path.dirname(os.path.dirname(archive_path))
        if not os.path.exists(filingYearDir):
            os.mkdir(filingYearDir)
        xbrl_archive.flush(create_parent_dir=True)
        metadata_df['filingDate'] = pd.to_datetime(metadata_df['filingDate'])
        metadata_df.sort_values(by='filingDate', inplace=True)
        metadata_df.to_csv(METADATA_FILENAME, index=False)
        xbrl_archive, archive_path = None, None  # make sure

    print('\nXBRL downloads END')
    print('\n%d xbrl\'s downloaded in this run. %d errors' % (n_downloaded, n_errors))
    print('Whole metadata_df: %d rows, %d errors'
          % (metadata_df.shape[0], metadata_df.loc[~metadata_df['outcome']].shape[0]))