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
    ca_files = glob.glob(os.path.join(CONFIG_DIR, '3_nse_cf_ca/CF-CA-*.csv'))
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

if __name__ == '__main__':
    for symbol in ['BRITANNIA', 'KBCGLOBAL', 'RADIOCITY', 'IRCTC', 'MOTOGENFIN', 'MARINE',
                   'AVANTIFEED']:
        xx = get_corporate_actions(symbol)
        print('%s\t%s' %(symbol, {'EX-DATE':list(xx['EX-DATE']), 'MULT':list(xx['MULT'])}))