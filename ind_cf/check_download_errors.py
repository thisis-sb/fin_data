import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import base_utils
import common.utils

# --------------------------------------------------------------------------------------------

if __name__ == '__main__':
    df = pd.read_csv('C:/Users/bhats/SUNILBHAT/10_data/01_fin_data/02_ind_cf/nse_download_errs2.csv')
    # df = df.head()

    error_df = pd.DataFrame()
    for idx, row in df.iterrows():
        print(common.utils.progress_str(idx + 1, df.shape[0]), end='')
        sys.stdout.flush()
        xbrl_url = row['XBRL Link']

        try:
            result = base_utils.download_xbrl_fr(xbrl_url)
            assert result['outcome'], 'download_xbrl_file failed for %s' % xbrl_url
        except Exception as e:
            error_df = pd.concat([
                error_df, pd.DataFrame(
                    {'NSE Symbol':row['NSE Symbol'],
                     'ISIN':row['ISIN'],
                     # 'SERIES':row['SERIES'],
                      # 'company_name':row['company_name'],
                     'company_name': row['COMPANY NAME'],
                     # 'period_end':row['period_end'],
                        'XBRL Link': xbrl_url, 'error_msg': e}, index=[0])])

    error_df.reset_index(drop=True, inplace=True)
    error_df.to_csv('C:/Users/bhats/SUNILBHAT/98_log/01_fin_data/02_ind_cf/error_df.csv', index=False)
    print('\nDone. error_df.shape:', error_df.shape)