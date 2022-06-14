import os
import common.utils as utils

WIP              = utils.folder_suffix(False)

DATA_ROOT        = os.getenv('DATA_DIR') + f'/01_fin_data/{WIP}'
CONFIG_DIR       = DATA_ROOT + f'/00_config'
LOG_DIR          = os.path.join(os.getenv('HOME_DIR') + f'/98_log_dir/fin_data/{WIP}')

NSE_ARCHIVES_URL     = 'https://archives.nseindia.com'
IND_CF_ARCHIVE_PATHS = {'bse_fr_2':os.path.join(DATA_ROOT, '02_ind_cf/bse_fr_2')}

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    print(WIP)
    print(DATA_ROOT)
    print(CONFIG_DIR)
    print(LOG_DIR)