import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import api.nse_symbols as nse_symbols
import base_utils
import download_fr

# ---------------------------------------------------------------------------------------------
if __name__ == '__main__':
    symbol = 'ABCAPITAL' if len(sys.argv) == 1 else sys.argv[1]

    xbrl_urls_df = download_fr.get_fr_xbrl_urls('bse', redo_errors=False)
    xbrl_urls_df = xbrl_urls_df.loc[xbrl_urls_df['ISIN'] == nse_symbols.get_isin(symbol)]
    print('xbrl_urls (bse):', xbrl_urls_df.shape[0])

    META_DATA_DB, ERRORS_DB = base_utils.fr_meta_data_files('bse')
    meta_data = pd.read_csv(META_DATA_DB)
    meta_data = meta_data.loc[meta_data['NSE Symbol'] == symbol]
    print('META_DATA_DB (bse):', meta_data['period'].unique())