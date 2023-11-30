"""
All indices. Currently NSE only.
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import sys
import pandas as pd

PATH_1 = CONFIG_ROOT
PATH_2 = os.path.join(DATA_ROOT, r'00_common\02_nse_indices')

''' --------------------------------------------------------------------------------------- '''
def list_indices(index_name=None):
    idx_list = pd.read_excel(os.path.join(PATH_1, '01_fin_data.xlsx'), sheet_name='indices')\
        .dropna().reset_index(drop=True)
    result_dict = {}
    if index_name is None:
        result_dict['all_indices'] = idx_list[['Symbol', 'Source', 'File Name']]
    else:
        filenames = dict(zip(idx_list['Symbol'], idx_list['File Name']))
        for ix in index_name:
            df = pd.read_csv(os.path.join(PATH_2, filenames[ix]))
            result_dict[ix] = df[['Symbol', 'Series', 'Company Name', 'Industry']]
    return result_dict

def index_sectors(index_name):
    df = pd.read_excel(os.path.join(PATH_1, '01_fin_data.xlsx'), sheet_name='indices')
    df = df.loc[df['Symbol'] == index_name]
    if df.shape[0] == 0:
        return []
    index_file_name = df['File Name'].values[0]
    df = pd.read_csv(os.path.join(PATH_2, index_file_name))
    return sorted(df['Industry'].unique())

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    idx_name = None if len(sys.argv) == 1 else sys.argv[1:]
    r = list_indices(idx_name)
    [print('\n%s :::\n%s\n' % (k, r[k].to_string(index=False))) for k in r.keys()]
    if idx_name is not None:
        print('\nIndex Sectors:::')
        [print('\n%s :::\n%s\n' % (ix, "\n".join(index_sectors(ix)))) for ix in idx_name]