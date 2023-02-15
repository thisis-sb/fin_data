"""
Corporate Actions (bonus & split) history for NSE Symbols
"""
''' --------------------------------------------------------------------------------------- '''

import glob
import os
import sys
import pandas as pd
import re

''' --------------------------------------------------------------------------------------- '''
def get_corporate_actions(symbol, cutoff_date='2018-01-01'):
    ca_files = glob.glob(os.path.join(os.getenv('CONFIG_ROOT'), '03_nse_cf_ca/CF_CA_*.csv'))
    ca_df = pd.concat([pd.read_csv(f) for f in ca_files], axis=0)
    ca_df['Ex Date'] = pd.to_datetime(pd.to_datetime(ca_df['Ex Date']), '%Y-%m-%d')
    ca_df = ca_df.loc[ca_df['Ex Date'] >= cutoff_date]
    ca_df = ca_df.loc[(ca_df['Series'] == 'EQ') & (ca_df['Symbol'] == symbol)]
    ca_df.reset_index(inplace=True, drop=True)

    ca_df['Purpose'] = ca_df['Purpose'].str.strip()
    ca_df = ca_df.loc[ca_df['Purpose'].str.contains('^Bonus') |\
                      ca_df['Purpose'].str.contains('^Face Value Split')]

    ca_df['Ex Date'] = pd.to_datetime(ca_df['Ex Date'])
    ca_df['Ex Date'] = ca_df['Ex Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    ca_df = ca_df.sort_values(by='Ex Date', ascending=True).reset_index(drop=True)
    ca_df = ca_df[['Symbol', 'Purpose', 'Ex Date']].reset_index(drop=True)

    ca_df['MULT'] = 1.0

    for idx, row in ca_df.iterrows():
        purpose = row['Purpose'].strip()
        purpose = purpose.replace('/', ' / ')
        mult    = row['MULT']
        if purpose[0:16] == 'Face Value Split':
            tok = re.split('Rs | Re ', row['Purpose'])
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
    return ca_df[['Symbol', 'Ex Date', 'MULT', 'Purpose']]

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
        date_idx = res_df.index[res_df['Date'] == cfca_row['Ex Date']]
        if len(date_idx) != 0:
            res_df = res_df.apply(lambda row: adj(row, date_idx[0], cfca_row['MULT']), axis=1)
    res_df.drop(columns=['idx'], inplace=True)
    return res_df

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    for symbol in ['BRITANNIA', 'KBCGLOBAL', 'RADIOCITY', 'IRCTC', 'MOTOGENFIN', 'MARINE',
                   'AVANTIFEED', 'TATASTEEL']:
        xx = get_corporate_actions(symbol)
        print('%s\t%s' % (symbol, {'Ex Date':list(xx['Ex Date']), 'MULT':list(xx['MULT'])}))