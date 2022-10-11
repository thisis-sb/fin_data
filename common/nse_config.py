"""
Get all NSE config files (symbols, indices, et al)
Usage: 1 or 2-year
"""

import os
import sys
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pygeneric.http_utils as html_utils
from settings import CONFIG_DIR

''' --------------------------------------------------------------------------------------- '''
def get_config(opt=None):
    # 1. NSE symbols & indices
    clean_cols = ['Symbol', 'ISIN', 'Series', 'Company Name', 'Face Value',
                  'Paid-Up Value', 'Market Lot', 'Listing Date', 'Industry', 'Underlying',
                  'ETF Name', 'Sr.No.', 'Instrument Type']
    nse_config_dicts = [
        {'urls': ['https://archives.nseindia.com/content/equities/EQUITY_L.csv'
                  ],
         'column_map': {'SYMBOL': clean_cols[0], ' ISIN NUMBER': clean_cols[1],
                        ' SERIES': clean_cols[2], 'NAME OF COMPANY': clean_cols[3],
                        ' PAID UP VALUE': clean_cols[5], ' FACE VALUE': clean_cols[4],
                        ' MARKET LOT': clean_cols[6], ' DATE OF LISTING': clean_cols[7]
                        }
         },
        {'urls': [
            'https://archives.nseindia.com/content/equities/List_of_Active_Securities_CM_DEBT.csv'
            ],
         'column_map': {'Sr.No.': clean_cols[11], 'ISIN': clean_cols[1],
                        'NAME OF COMPANY': clean_cols[3], 'Instrument Type': clean_cols[12]
                        }
         },
        {'urls': ['https://archives.nseindia.com/content/indices/ind_nifty50list.csv',
                  'https://archives.nseindia.com/content/indices/ind_nifty100list.csv',
                  'https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv',
                  'https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv',
                  'https://archives.nseindia.com/content/indices/ind_nifty500list.csv',
                  'https://archives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv'
                  ],
         'column_map': {'Symbol': clean_cols[0], 'ISIN Code': clean_cols[1],
                        'Series': clean_cols[2], 'Company Name': clean_cols[3],
                        'Industry': clean_cols[8]
                        }
         }
    ]

    for ix in nse_config_dicts:
        for url in ix['urls']:
            print('Downloading', os.path.basename(url), end=' ... ')
            df = pd.read_csv(url)
            df.rename(columns=ix['column_map'], inplace=True)
            df = df[list(ix['column_map'].values())]
            df.to_csv(CONFIG_DIR + '/01_nse_symbols/%s' % os.path.basename(url), index=False)
            print('Done, shape:', df.shape)

    # 2. NSE ETF list
    print('Downloading eq_etfseclist.csv', end=' ... ')
    url = 'https://archives.nseindia.com/content/equities/eq_etfseclist.csv'
    cols_map = {'Symbol': clean_cols[0], 'ISIN Number': clean_cols[1],
                'Underlying': clean_cols[9], 'Security Name': clean_cols[10],
                ' Face Value': clean_cols[4], ' Market Lot': clean_cols[6],
                ' Date of Listing': clean_cols[7]
                }
    df = pd.read_excel(url, sheet_name='in')
    df.rename(columns=cols_map, inplace=True)
    df = df[list(cols_map.values())]
    df.to_csv(CONFIG_DIR + '/01_nse_symbols/%s' % os.path.basename(url), index=False)
    print('Done, shape:', df.shape)

    # 3. NSE symbol changes
    print('Downloading symbolchange.csv', end=' ... ')
    url = 'https://archives.nseindia.com/content/equities/symbolchange.csv'
    cols_map = {'Symbol': clean_cols[0], 'ISIN Number': clean_cols[1],
                'Underlying': clean_cols[9], 'Security Name': clean_cols[10],
                ' Face Value': clean_cols[4], ' Market Lot': clean_cols[6],
                ' Date of Listing': clean_cols[7]
                }
    df = pd.read_csv(url, encoding='cp1252', header=None,
                     names=[clean_cols[3], 'Old Symbol', 'New Symbol', 'Date of Change'])
    df.to_csv(CONFIG_DIR + '/01_nse_symbols/%s' % os.path.basename(url), index=False)
    print('Done, shape:', df.shape)

    # 4. FO market lots - postpone for now. Needed for FO?
    """print('Downloading fo_mktlots.csv', end=' ... ')
    nse_symbol_changes_url = 'https://archives.nseindia.com/content/fo/fo_mktlots.csv'
    df = pd.read_csv(nse_symbol_changes_url, encoding='cp1252')
    df.to_csv(CONFIG_DIR + '/01_nse_symbols/fo_mktlots.csv', index=False)
    print('Done, shape:', df.shape)"""

    # 5. ------- TO DO: BSE_CODES -------
    # -----------------------------------

    # 6. Prepare list of symbol & index membership
    print('Preparing symbols_master.csv', end=' ... ')
    equity_l_df = pd.read_csv(os.path.join(CONFIG_DIR, '01_nse_symbols/EQUITY_L.csv'))
    equity_l_df.rename(columns=lambda x: x.strip(), inplace=True)
    equity_l_df = equity_l_df[['Symbol', 'Company Name', 'Series', 'ISIN']]

    indices = {'NIFTY 50': 'ind_nifty50list',
               'NIFTY 100': 'ind_nifty100list',
               'MIDCAP 150': 'ind_niftymidcap150list',
               'SMALLCAP 250': 'ind_niftysmallcap250list',
               'MICROCAP 250': 'ind_niftymicrocap250_list'
               }
    indices_master = pd.DataFrame()
    for index_name in list(indices.keys()):
        df = pd.read_csv(os.path.join(CONFIG_DIR, f'01_nse_symbols/{indices[index_name]}.csv'))
        df['nse_index_name'] = index_name
        if indices_master.shape[0] > 0:
            df = df[~df['ISIN'].isin(indices_master['ISIN'])]
            df.reset_index(drop=True, inplace=True)
        indices_master = pd.concat([indices_master, df])

    indices_master.drop(columns=['Company Name'], inplace=True)
    df = pd.merge(equity_l_df, indices_master, on=['Symbol', 'ISIN', 'Series'], how='left')
    df.to_csv(os.path.join(CONFIG_DIR, '01_nse_symbols/symbols_master.csv'), index=False)
    print('Done, shape:', df.shape)
    return

def get_cf_ca(opt):
    year = opt.split('-')[1]
    from datetime import datetime
    date_now = datetime.now()
    assert int(year) <= date_now.year, 'Invalid Year %s' % year
    if int(year) == date_now.year:
        from_to = ['01-01-%d' % int(year),'%s-%s-%d' % (('%d' % date_now.day).zfill(2),
                                                        ('%d' % date_now.month).zfill(2),
                                                        int(year))]
    else:
        from_to = ['01-01-%d' % int(year), '31-12-%d' % int(year)]
    url = 'https://www.nseindia.com/api/corporates-corporateActions?index=equities' + \
        '&from_date=%s&to_date=%s' % (from_to[0], from_to[1])
    print('Downloading %s ...' % from_to, end='')
    cf_ca_json = html_utils.http_get(url)
    cf_ca_df = pd.DataFrame(cf_ca_json)
    cf_ca_df.reset_index(drop=True, inplace=True)
    cols = {
        'symbol':'Symbol',
        'series':'Series',
        'isin': 'ISIN',
        'faceVal':'Face Value',
        'subject':'Purpose',
        'exDate':'Ex Date',
        'recDate':'Record Date',
        'bcStartDate': 'BC Start Date',
        'bcEndDate': 'BC End Date',
        'comp': 'Company Name',
        'ndStartDate':'ndStartDate', 'ndEndDate':'ndEndDate', 'caBroadcastDate':'caBroadcastDate'

    }
    cf_ca_df.rename(columns=cols, inplace=True)
    cf_ca_df = cf_ca_df[list(cols.values())]
    cf_ca_df.to_csv(os.path.join(CONFIG_DIR, '02_nse_cf_ca/CF_CA_%s.csv' % year), index=False)
    if len(cf_ca_json) != cf_ca_df.shape[0]:
        print('Warning! shapes are not matching. %d/%d' % (len(cf_ca_json), cf_ca_df.shape[0]))
    else:
        print('Done. cf_ca_df.shape:', cf_ca_df.shape)
    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    opt = '1' if len(sys.argv) == 1 else sys.argv[1]
    opts = {
        '1':get_config,
        '2':get_cf_ca
    }

    print('\n---> opt =', opt, ':::')
    opts[opt.split('-')[0]](opt)
    print('---> Done :::')


