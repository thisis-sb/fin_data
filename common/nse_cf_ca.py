# --------------------------------------------------------------------------------------------
# Corporate Actions (bonus & split) history for NSE Symbols
# --------------------------------------------------------------------------------------------

import glob
import os
import sys
import pandas as pd
import re
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from settings import CONFIG_DIR

def get_corporate_actions(symbol):
    ca_files = glob.glob(os.path.join(CONFIG_DIR, '03_nse_cf_ca/CF-CA-*.csv'))
    ca_df = pd.concat([pd.read_csv(f) for f in ca_files], axis=0)
    # ca_df.to_csv(os.getenv('HOME_DIR') + '/98_output_dir/fin_data/df.csv', index=False)

    ca_df = ca_df.loc[(ca_df['SERIES'] == 'EQ') & (ca_df['SYMBOL'] == symbol)]

    ca_df['PURPOSE'] = ca_df['PURPOSE'].str.strip()
    ca_df = ca_df.loc[ca_df['PURPOSE'].str.contains('^Bonus') |\
                      ca_df['PURPOSE'].str.contains('^Face Value Split')]

    ca_df['EX-DATE'] = pd.to_datetime(ca_df['EX-DATE'])
    ca_df['EX-DATE'] = ca_df['EX-DATE'].apply(lambda x: x.strftime('%Y-%m-%d'))
    ca_df = ca_df.sort_values(by='EX-DATE', ascending=True).reset_index(drop=True)
    ca_df = ca_df[['SYMBOL', 'PURPOSE', 'EX-DATE']].reset_index(drop=True)

    ca_df['MULT'] = 1.0

    for idx, row in ca_df.iterrows():
        purpose = row['PURPOSE'].strip()
        mult    = row['MULT']
        if purpose[0:16] == 'Face Value Split':
            tok = re.split('Rs | Re ', row['PURPOSE'])
            try:
                mult *= float(tok[1].split('/-')[0]) / float(tok[2].split('/-')[0])
            except:
                mult *= float(tok[1].split('Per')[0]) / float(tok[2].split('Per')[0])
        elif purpose[0:5] == 'Bonus':
            tok = purpose.split()[1].split(':')
            mult *= (float(tok[0]) + float(tok[1])) / float(tok[1])
        else:
            assert 1 == 0
        ca_df.loc[idx, 'MULT'] = mult
    return ca_df[['SYMBOL', 'EX-DATE', 'MULT', 'PURPOSE']]

def get_cf_ca_mult(symbol, dates_series):
    res_df = pd.DataFrame({'Date':dates_series})
    res_df['MULT'] = 1.0
    res_df['DIV']  = 1.0
    res_df['idx'] = res_df.index

    ca_df = get_corporate_actions(symbol)
    if ca_df.shape[0] == 0:
        return res_df

    def adj(df_row, date_idx, mult):
        df_row['MULT'] = df_row['MULT'] / mult if df_row['idx'] < date_idx else df_row['MULT']
        df_row['DIV']  = df_row['DIV'] * mult if df_row['idx'] < date_idx else df_row['DIV']
        return df_row

    for idx, cfca_row in ca_df.iterrows():
        date_idx = res_df.index[res_df['Date'] == cfca_row['EX-DATE']]
        if len(date_idx) != 0:
            res_df = res_df.apply(lambda row: adj(row, date_idx[0], cfca_row['MULT']), axis=1)
    res_df.drop(columns=['idx'], inplace=True)
    return res_df

if __name__ == '__main__':
    for symbol in ['BRITANNIA', 'KBCGLOBAL', 'RADIOCITY', 'IRCTC', 'MOTOGENFIN', 'MARINE',
                   'AVANTIFEED', 'TATASTEEL']:
        xx = get_corporate_actions(symbol)
        print('%s\t%s' %(symbol, {'EX-DATE':list(xx['EX-DATE']), 'MULT':list(xx['MULT'])}))

    import nse_spot
    for sym in ['TATASTEEL', 'RELIANCE']:
        print()
        df = nse_spot.NseSpotPVData().get_pv_data(symbol=sym,
                                                  from_to=['2022-07-01', '2022-08-05'],
                                                  adjust=False)
        df = pd.merge(df, get_cf_ca_mult(sym, df['Date']), on='Date', how='left')
        print(df[['Date', 'Close', 'Volume', 'MULT', 'DIV']].tail(10))