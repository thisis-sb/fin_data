import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import base.common

'''WIP        = base.common.folder_suffix(True)
ROOT_DIR   = os.getenv('DATA_DIR') + f'/01_fin_data/{WIP}'
CONFIG_DIR = ROOT_DIR + f'/00_config/{WIP}'
LOG_DIR    = os.path.join(os.getenv('HOME_DIR') + f'/98_output_dir/{WIP}/fin_data_ind_cf')

def meta_data_files(exchange):
    META_DATA_DB = ROOT_DIR + f'/02_ind_cf/{exchange}_fr_metadata.csv'
    ERRORS_DB    = ROOT_DIR + f'/02_ind_cf/{exchange}_fr_errors.csv'
    return META_DATA_DB, ERRORS_DB

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    print(WIP)
    print(CONFIG_DIR)
    print(ROOT_DIR)
    print(LOG_DIR)'''