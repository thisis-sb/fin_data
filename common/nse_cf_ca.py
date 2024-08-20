"""
Corporate Actions (bonus & split) history for NSE Symbols
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import glob
import os
import sys
import pandas as pd
import re

PATH_1 = os.path.join(DATA_ROOT, '00_common/03_nse_cf_ca')

''' --------------------------------------------------------------------------------------- '''
class NseCorporateActions:
    def __init__(self, verbose=False):
        self.verbose = verbose
        ca_files = glob.glob(os.path.join(PATH_1, 'CF_CA_*.csv'))
        df = pd.concat([pd.read_csv(f) for f in ca_files], axis=0)
        df = pd.concat([pd.read_csv(f) for f in ca_files], axis=0)
        df['Ex Date'] = pd.to_datetime(pd.to_datetime(df['Ex Date']), '%Y-%m-%d')
        self.cf_data = df.reset_index(drop=True)
        if self.verbose:
            print('NseCorporateActions: cf_data', self.cf_data.shape)
        self.cf_mult_cache = {}
        self.__status__ = True

    def get_history(self, symbol, cutoff_date='2018-01-01', prettyprint=False):
        df = self.cf_data.loc[self.cf_data['Ex Date'] >= cutoff_date]
        df = df.loc[(df['Series'] == 'EQ') & (df['Symbol'] == symbol)].reset_index(drop=True)
        df['Purpose'] = df['Purpose'].str.strip()

        if self.verbose:
            print('NseCorporateActions.get_history(%s): %s' % (symbol, df.shape))
        if not prettyprint:
            return df[['Symbol', 'Series', 'Face Value', 'Ex Date', 'Record Date', 'Purpose']]
        else:
            df['Face Value'] = df['Face Value'].astype(int)
            xx = ''
            for ix, row in df.iterrows():
                max_width = 60
                xx = xx + '%d) Ex/Record Dates: %s / %s\n%s\n\n' \
                     % (ix, row['Ex Date'].strftime('%d-%b-%Y'), row['Record Date'],
                        row['Purpose'])
            return xx

    def get_cf_ca_multipliers(self, symbol, cutoff_date='2018-01-01', cache=True):
        cache_key = '%s-%s' % (symbol, cutoff_date)
        if cache and cache_key in self.cf_mult_cache.keys():
            return self.cf_mult_cache[cache_key]

        ca_df = self.get_history(symbol, cutoff_date=cutoff_date)
        ca_df = ca_df.loc[ca_df['Purpose'].str.contains('^Bonus') |\
                          ca_df['Purpose'].str.contains('^Face Value Split')]

        ca_df['Ex Date'] = pd.to_datetime(ca_df['Ex Date'])
        ca_df['Ex Date'] = ca_df['Ex Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        ca_df = ca_df.sort_values(by='Ex Date', ascending=True).reset_index(drop=True)
        ca_df = ca_df[['Symbol', 'Purpose', 'Ex Date']].reset_index(drop=True)

        ca_df['MULT'] = 1.0

        for idx, row in ca_df.iterrows():
            purpose = row['Purpose'].strip()
            purpose = purpose.replace('/', ' / ').replace('Rs', ' Rs ').replace('Re', ' Re ')
            mult    = row['MULT']
            if purpose[0:16] == 'Face Value Split':
                tok = re.split('Rs | Re ', purpose)
                # if symbol == 'NESTLEIND': print('tok:::', tok)
                try:
                    mult *= float(tok[1].split(' / -')[0].strip()) / \
                            float(tok[2].split(' / -')[0].strip())
                except:
                    try:
                        mult *= float(tok[1].split('Per')[0].strip()) / \
                                float(tok[2].split('Per')[0].strip())
                    except:
                        assert False, '[%s] [%s] [%s]' % (row['Symbol'], row['Ex Date'], purpose)
            elif purpose[0:5] == 'Bonus':
                try:
                    tok = purpose.split()[1].split(':')
                    mult *= (float(tok[0].strip()) + float(tok[1].strip())) / float(tok[1].strip())
                except:
                    try:
                        x = purpose.split(' ')
                        tok = x[-1].split(':')
                        mult *= (float(tok[0].strip()) + float(tok[1].strip())) / float(tok[1].strip())
                    except:
                        assert False, '[%s] [%s] [%s]' % (row['Symbol'], row['Ex Date'], purpose)
            else:
                assert 1 == 0
            ca_df.loc[idx, 'MULT'] = mult
        if cache:
            self.cf_mult_cache[cache_key] = ca_df[['Symbol', 'Ex Date', 'MULT', 'Purpose']]
        return ca_df[['Symbol', 'Ex Date', 'MULT', 'Purpose']]

def test_me():
    test_dates = ['2018-01-01', '2023-03-31']
    test_data = {
        'BRITANNIA': {'Ex Date': ['2018-11-29'], 'MULT': [2.0]},
        'KBCGLOBAL': {'Ex Date': ['2020-07-02', '2021-08-12', '2021-08-12'],'MULT': [5.0, 2.0, 5.0]},
        'RADIOCITY': {'Ex Date': ['2019-02-20', '2020-03-12'], 'MULT': [5.0, 1.25]},
        'IRCTC': {'Ex Date': ['2021-10-28'], 'MULT': [5.0]},
        'MOTOGENFIN': {'Ex Date': ['2020-06-19'], 'MULT': [2.0]},
        'MARINE': {'Ex Date': ['2021-02-18'], 'MULT': [5.0]},
        'AVANTIFEED': {'Ex Date': ['2018-06-26'], 'MULT': [1.5]},
        'TATASTEEL': {'Ex Date': ['2022-07-28'], 'MULT': [10.0]}
    }

    nse_ca_obj = NseCorporateActions()
    print('\nTesting nse_cf_ca ...', end=' ')
    for symbol in test_data.keys():
        x1 = nse_ca_obj.get_cf_ca_multipliers(symbol, cutoff_date=test_dates[0])
        x1 = x1.loc[x1['Ex Date'] <= test_dates[1]]
        x2 = {'Ex Date':list(x1['Ex Date']), 'MULT':list(x1['MULT'])}
        assert x2 == test_data[symbol], 'ERROR! cf_ca_multpliers not matching, %s/%s' \
                                        % (x2, test_data[symbol])
    print('OK')
    return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    test_me()
    print('See symbol_info.py for use')