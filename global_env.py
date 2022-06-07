import os
import base.common

WIP              = base.common.folder_suffix(False)

DATA_ROOT        = os.getenv('DATA_DIR') + f'/01_fin_data/{WIP}'
CONFIG_DIR       = DATA_ROOT + f'/00_config'
LOG_DIR          = os.path.join(os.getenv('HOME_DIR') + f'/98_log_dir/fin_data/{WIP}')

NSE_ARCHIVES_URL = 'https://archives.nseindia.com'

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    print(WIP)
    print(DATA_ROOT)
    print(CONFIG_DIR)
    print(LOG_DIR)