"""
Get all NSE config files (symbols, indices, et al)
Usage: None or a list of years
"""
''' --------------------------------------------------------------------------------------- '''

import os
import sys
import pandas as pd
from datetime import date
import pygeneric.http_utils as pyg_http_utils

PATH_1 = os.path.join(os.getenv('CONFIG_ROOT'), '01_nse_symbols')
PATH_2 = os.path.join(os.getenv('CONFIG_ROOT'), '02_nse_indices')
PATH_3 = os.path.join(os.getenv('CONFIG_ROOT'), '03_nse_cf_ca')
PATH_4 = os.path.join(os.getenv('CONFIG_ROOT'), '05_portfolio')

''' --------------------------------------------------------------------------------------- '''
def symbols_and_broad_indices():
    clean_cols = ['Symbol', 'ISIN', 'Series', 'Company Name', 'Face Value',
                  'Paid-Up Value', 'Market Lot', 'Listing Date', 'Industry', 'Underlying',
                  'ETF Name', 'Sr.No.', 'Instrument Type']
    nse_config_dicts = [
        {
            'urls': ['https://archives.nseindia.com/content/equities/EQUITY_L.csv'
                     ],
            'column_map': {'SYMBOL': clean_cols[0], ' ISIN NUMBER': clean_cols[1],
                           ' SERIES': clean_cols[2], 'NAME OF COMPANY': clean_cols[3],
                           ' PAID UP VALUE': clean_cols[5], ' FACE VALUE': clean_cols[4],
                           ' MARKET LOT': clean_cols[6], ' DATE OF LISTING': clean_cols[7]
                           },
            'storage_path': PATH_1
        },
        {
            'urls': [
                'https://archives.nseindia.com/content/equities/List_of_Active_Securities_CM_DEBT.csv'
                ],
            'column_map': {'Sr.No.': clean_cols[11], 'ISIN': clean_cols[1],
                           'NAME OF COMPANY': clean_cols[3], 'Instrument Type': clean_cols[12]
                           },
            'storage_path': PATH_1
        },
        {
            'urls': ['https://archives.nseindia.com/content/indices/ind_nifty50list.csv',
                     'https://archives.nseindia.com/content/indices/ind_niftynext50list.csv',
                     'https://archives.nseindia.com/content/indices/ind_nifty100list.csv',
                     'https://archives.nseindia.com/content/indices/ind_nifty200list.csv',
                     'https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv',
                     'https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv',
                     'https://archives.nseindia.com/content/indices/ind_nifty500list.csv',
                     'https://archives.nseindia.com/content/indices/ind_niftymicrocap250_list.csv',
                     'https://archives.nseindia.com/content/indices/ind_niftytotalmarket_list.csv'
                     ],
            'column_map': {'Symbol': clean_cols[0], 'ISIN Code': clean_cols[1],
                           'Series': clean_cols[2], 'Company Name': clean_cols[3],
                           'Industry': clean_cols[8]
                           },
            'storage_path': PATH_2
        }
    ]

    for ix in nse_config_dicts:
        for url in ix['urls']:
            print('Downloading', os.path.basename(url), end=' ... ')
            df = pd.read_csv(url)
            df.rename(columns=ix['column_map'], inplace=True)
            df = df[list(ix['column_map'].values())]
            df.to_csv(os.path.join(ix['storage_path'], '%s' % os.path.basename(url)), index=False)
            print('Done, shape:', df.shape)

    return

def sectoral_indices():
    print('Downloading sectoral indices ...', end=' ')
    indices = [
        'NIFTY BANK',
        'NIFTY AUTO',
        'NIFTY FINANCIAL SERVICES',
        'NIFTY FMCG',
        'NIFTY IT',
        'NIFTY MEDIA',
        'NIFTY METAL',
        'NIFTY PHARMA',
        'NIFTY PSU BANK',
        'NIFTY PRIVATE BANK',
        'NIFTY REALTY',
        'NIFTY HEALTHCARE INDEX',
        'NIFTY CONSUMER DURABLES',
        'NIFTY OIL & GAS'
    ]
    http_obj = pyg_http_utils.HttpDownloads()

    for index_name in indices:
        url = 'https://www.nseindia.com/api/equity-stockIndices' + \
              '?index=%s' % index_name.replace(' ', '%20').replace('&', '%26')
        data = http_obj.http_get_json(url)
        df = []
        for x in data['data']:
            if x['priority'] == 0 and x['symbol'] == x['meta']['symbol']:
                df.append({'Symbol': x['symbol'],
                           'ISIN': x['meta']['isin'],
                           'Series': x['series'],
                           'Company Name': x['meta']['companyName'],
                           'Industry': x['meta']['industry'] if 'industry' in x[
                               'meta'].keys() else None
                           })
        df = pd.DataFrame(df)
        df.to_csv(os.path.join(PATH_2, 'sect_ind_%s.csv' % index_name.replace(' ', '_')), index=False)
    print('Done')
    return

def get_etf_list():
    print('Downloading eq_etfseclist.csv', end=' ... ')
    clean_cols = ['Symbol', 'ISIN', 'Series', 'Company Name', 'Face Value',
                  'Paid-Up Value', 'Market Lot', 'Listing Date', 'Industry', 'Underlying',
                  'ETF Name', 'Sr.No.', 'Instrument Type']
    url = 'https://archives.nseindia.com/content/equities/eq_etfseclist.csv'
    cols_map = {'Symbol': clean_cols[0], 'ISIN Number': clean_cols[1],
                'Underlying': clean_cols[9], 'Security Name': clean_cols[10],
                ' Face Value': clean_cols[4], ' Market Lot': clean_cols[6],
                ' Date of Listing': clean_cols[7]
                }
    # before - as csv was in fact an excel --> df = pd.read_excel(url, sheet_name='in')
    df = pd.read_csv(url, encoding='cp1252')
    df.rename(columns=cols_map, inplace=True)
    df = df[list(cols_map.values())]
    df.to_csv(os.path.join(PATH_1, os.path.basename(url)), index=False)
    print('Done, shape:', df.shape)
    return

def get_symbol_changes():
    print('Downloading symbolchange.csv', end=' ... ')
    clean_cols = ['Symbol', 'ISIN', 'Series', 'Company Name', 'Face Value',
                  'Paid-Up Value', 'Market Lot', 'Listing Date', 'Industry', 'Underlying',
                  'ETF Name', 'Sr.No.', 'Instrument Type']
    url = 'https://archives.nseindia.com/content/equities/symbolchange.csv'
    cols_map = {'Symbol': clean_cols[0], 'ISIN Number': clean_cols[1],
                'Underlying': clean_cols[9], 'Security Name': clean_cols[10],
                ' Face Value': clean_cols[4], ' Market Lot': clean_cols[6],
                ' Date of Listing': clean_cols[7]
                }
    df = pd.read_csv(url, encoding='cp1252', header=None,
                     names=[clean_cols[3], 'Old Symbol', 'New Symbol', 'Date of Change'])
    df.to_csv(os.path.join(PATH_1, os.path.basename(url)), index=False)
    print('Done, shape:', df.shape)
    return

def get_misc():
    print('get_misc: Nothing for now, but later')
    # 4. FO market lots - postpone for now. Needed for FO?
    """print('Downloading fo_mktlots.csv', end=' ... ')
    nse_symbol_changes_url = 'https://archives.nseindia.com/content/fo/fo_mktlots.csv'
    df = pd.read_csv(nse_symbol_changes_url, encoding='cp1252')
    df.to_csv(CONFIG_DIR + '/01_nse_symbols/fo_mktlots.csv', index=False)
    print('Done, shape:', df.shape)"""
    return

def prepare_symbols_master():
    print('Preparing symbols_master.csv', end=' ... ')
    equity_l_df = pd.read_csv(os.path.join(PATH_1, 'EQUITY_L.csv'))
    equity_l_df.rename(columns=lambda x: x.strip(), inplace=True)
    equity_l_df = equity_l_df[['Symbol', 'Company Name', 'Series', 'ISIN']]

    indices = {'NIFTY 50': 'ind_nifty50list',
               'NEXT 50': 'ind_niftynext50list',
               'MIDCAP 150': 'ind_niftymidcap150list',
               'SMALLCAP 250': 'ind_niftysmallcap250list',
               'MICROCAP 250': 'ind_niftymicrocap250_list'
               }
    indices_master = pd.DataFrame()
    for index_name in list(indices.keys()):
        df = pd.read_csv(os.path.join(PATH_2, f'{indices[index_name]}.csv'))
        df['nse_index_name'] = index_name
        if indices_master.shape[0] > 0:
            df = df[~df['ISIN'].isin(indices_master['ISIN'])]
            df.reset_index(drop=True, inplace=True)
        indices_master = pd.concat([indices_master, df])

    indices_master.drop(columns=['Company Name'], inplace=True)
    df = pd.merge(equity_l_df, indices_master, on=['Symbol', 'ISIN', 'Series'], how='left')
    df['nse_index_name'] = df['nse_index_name'].fillna('xxxxx')
    df.to_csv(os.path.join(PATH_1, 'symbols_master.csv'), index=False)
    print('Done, shape:', df.shape)
    return

def custom_indices():
    x1 = pd.read_csv(os.path.join(PATH_1, 'symbols_master.csv'))
    x2 = pd.read_excel(os.path.join(PATH_4, '0_STOCKS_DB.xlsx'),
                       sheet_name='WLs', skiprows=1,
                       usecols=['Symbol', 'Sector', 'Series', 'WL#', 'Target'])
    x2 = x2[x2['WL#'].notna()]
    x2['WL#'] = x2['WL#'].astype(int).astype(str)
    x2 = x2.loc[x2['WL#'] == '1']
    x1 = x1.loc[x1['Symbol'].isin(x2['Symbol'].unique())]
    x1[['Symbol', 'ISIN', 'Series', 'Company Name', 'Industry']].\
        to_csv(os.path.join(PATH_2, 'watchlist_1.csv'), index=False)
    return

def download_cf_ca(year):
    date_today = date.today()
    assert int(year) <= date_today.year, 'Invalid Year %s' % year
    if int(year) == date_today.year:
        from_to = ['01-01-%d' % int(year),'%s-%s-%d' % (('%d' % date_today.day).zfill(2),
                                                        ('%d' % date_today.month).zfill(2),
                                                        int(year))]
    else:
        from_to = ['01-01-%d' % int(year), '31-12-%d' % int(year)]
    url = 'https://www.nseindia.com/api/corporates-corporateActions?index=equities' + \
        '&from_date=%s&to_date=%s' % (from_to[0], from_to[1])

    print('Downloading %s ...' % from_to, end='')
    http_obj = pyg_http_utils.HttpDownloads()
    cf_ca_json = http_obj.http_get_json(url)
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
    cf_ca_df.to_csv(os.path.join(PATH_3, 'CF_CA_%s.csv' % year), index=False)
    if len(cf_ca_json) != cf_ca_df.shape[0]:
        print('Warning! shapes are not matching. %d/%d' % (len(cf_ca_json), cf_ca_df.shape[0]))
    else:
        print('Done. cf_ca_df.shape:', cf_ca_df.shape)
    return

def last_n_pe_dates(n):
    today_str = date.today().strftime('%Y-%m-%d')
    pe_dates = []
    for yr in range(2018, date.today().year + 1):
        for dt in ['%d-03-31' % yr, '%d-06-30' % yr, '%d-09-30' % yr, '%d-12-31' % yr]:
            if dt <= today_str:
                pe_dates.append(dt)
    return pe_dates[-n:]

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    years = None if len(sys.argv) == 1 else [int(y) for y in sys.argv[1:]]

    if years is None:
        symbols_and_broad_indices()
        sectoral_indices()
        get_etf_list()
        get_symbol_changes()
        get_misc()
        prepare_symbols_master()
        custom_indices()
    else:
        for year in years:
            download_cf_ca(year)

    print('Last 6 pe_dates:', last_n_pe_dates(6))