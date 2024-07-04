"""
Get all NSE config files (symbols, indices, et al)
Usage: None or a list of years
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import sys
import pandas as pd
from datetime import date, datetime
import pygeneric.http_utils as pyg_http_utils
import pygeneric.misc as pyg_misc

PATH_0 = CONFIG_ROOT
PATH_1 = os.path.join(DATA_ROOT, '00_common/01_nse_symbols')
PATH_2 = os.path.join(DATA_ROOT, '00_common/02_nse_indices')
PATH_3 = os.path.join(DATA_ROOT, '00_common/03_nse_cf_ca')
PATH_4 = os.path.join(DATA_ROOT, '00_common/04_nse_cf_shp')

''' --------------------------------------------------------------------------------------- '''
def get_all_symbols():
    clean_cols = ['Symbol', 'ISIN', 'Series', 'Company Name', 'Face Value',
                  'Paid-Up Value', 'Market Lot', 'Listing Date', 'Industry', 'Underlying',
                  'ETF Name', 'Sr.No.', 'Instrument Type']
    nse_config_dicts = [
        {
            'urls': ['https://archives.nseindia.com/content/equities/EQUITY_L.csv'],
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

def get_all_indices(verbose=False):
    print('Downloading index components ...')
    if verbose:
        import json
    ''' Step1: get all indices data '''
    http_obj = pyg_http_utils.HttpDownloads(website='nse')
    url = 'https://www.nseindia.com/api/allIndices'
    data = http_obj.http_get_json(url)
    assert len(data) > 0 and 'data' in data.keys() and len(data['data']) > 0, \
        'Cannot continue (Try Later)! Empty or corrputed data: %s' % data
    if verbose:
        print(json.dumps(data, indent=2))
    ''' NOTE: for NSE indices, I am using Index Name as Symbol (for harmony) '''
    cols = {'index': 'Symbol', 'indexSymbol': 'short_symbol', 'key': 'category'}
    idx_df = pd.DataFrame(data['data'])[list(cols.keys())]
    idx_df.rename(columns=cols, inplace=True)
    idx_df.reset_index(drop=True, inplace=True)

    ''' Step1: get components of all indices '''
    broad_indices = get_broad_indices()
    for ix, row in idx_df.iterrows():
        pyg_misc.print_progress_str(ix + 1, idx_df.shape[0])
        symbol = row['Symbol']
        if ':' in symbol or '/' in symbol or \
                symbol in ['INDIA VIX', 'NIFTY50 TR 2X LEV', 'NIFTY50 PR 2X LEV']:
            continue
        if symbol in broad_indices.keys():
            df = broad_indices[symbol]
        else:
            url = 'https://www.nseindia.com/api/equity-stockIndices?index=%s' % row['short_symbol']
            url.replace(' ', '%20')
            data = http_obj.http_get_json(url)
            if len(data) == 0 or 'data' not in data.keys() or len(data['data']) == 0:
                continue
            if verbose:
                print(json.dumps(data, indent=2))
            x1 = [x for x in data['data'] if x['priority'] == 0]
            x2 = [x['meta'] for x in data['data'] if 'meta' in x.keys()]
            df1 = pd.DataFrame(x1)[['symbol', 'series', 'identifier']]
            df2 = pd.DataFrame(x2)[['symbol', 'isin', 'companyName', 'industry']]
            df = pd.merge(df2, df1, on='symbol', how='left')
            df.rename(columns={'symbol':'Symbol', 'isin':'ISIN', 'series':'Series',
                               'companyName':'Company Name', 'industry':'Industry',
                               'identifier':'Identifier'}, inplace=True)
        df.to_csv(os.path.join(PATH_2, '%s.csv' % symbol), index=False)
        idx_df.loc[ix, 'file_name'] = '%s.csv' % symbol

    idx_df.to_csv(os.path.join(PATH_2, 'all_nse_indices.csv'), index=False)
    print('\nDone. %d indices, %d categories, %d indices with no symbols.'
          % (idx_df.shape[0], len(idx_df['category'].unique()),
             idx_df[idx_df['file_name'].isnull()].shape[0]))

    return

def get_broad_indices():
    clean_cols = ['Symbol', 'ISIN', 'Series', 'Company Name', 'Face Value',
                  'Paid-Up Value', 'Market Lot', 'Listing Date', 'Industry', 'Underlying',
                  'ETF Name', 'Sr.No.', 'Instrument Type']
    nse_config_dicts = [
        {
            'symbols': ['NIFTY 50', 'NIFTY NEXT 50', 'NIFTY 100', 'NIFTY 200',
                        'NIFTY MIDCAP 150', 'NIFTY SMALLCAP 250', 'NIFTY 500',
                        'NIFTY MICROCAP 250', 'NIFTY TOTAL MARKET'],
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

    results = {}
    for ix in nse_config_dicts:
        for i, symbol in enumerate(ix['symbols']):
            url = ix['urls'][i]
            print('Downloading %s: %s' % (symbol, os.path.basename(url)) , end=' ... ')
            df = pd.read_csv(url)
            df.rename(columns=ix['column_map'], inplace=True)
            df = df[list(ix['column_map'].values())]
            f = '%s.csv' % symbol
            results[symbol] = df
            print('Done, shape:', df.shape)

    return results

''' Not used anymore. Keep for now '''
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
    url = 'https://archives.nseindia.com/content/equities/eq_etfseclist.csv'
    # before - as csv was in fact an excel --> df = pd.read_excel(url, sheet_name='in')
    df = pd.read_csv(url, encoding='cp1252')

    """
    # 2024-07-01 - columns got changed.
    clean_cols = ['Symbol', 'ISIN', 'Series', 'Company Name', 'Face Value',
                  'Paid-Up Value', 'Market Lot', 'Listing Date', 'Industry', 'Underlying',
                  'ETF Name', 'Sr.No.', 'Instrument Type']
    cols_map = {'Symbol': clean_cols[0], 'ISIN Number': clean_cols[1],
                'Underlying': clean_cols[9], 'Security Name': clean_cols[10],
                ' Face Value': clean_cols[4], ' Market Lot': clean_cols[6],
                ' Date of Listing': clean_cols[7]
                }
    df.rename(columns=cols_map, inplace=True)
    df = df[list(cols_map.values())]
    """
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

    indices = ['NIFTY 50', 'NIFTY NEXT 50',
               'NIFTY MIDCAP 150', 'NIFTY SMALLCAP 250', 'NIFTY MICROCAP 250']
    indices_master = pd.DataFrame()
    for index_name in indices:
        df = pd.read_csv(os.path.join(PATH_2, f'{index_name}.csv'))
        df['NSE Index'] = index_name
        if indices_master.shape[0] > 0:
            df = df[~df['ISIN'].isin(indices_master['ISIN'])]
            df.reset_index(drop=True, inplace=True)
        indices_master = pd.concat([indices_master, df])
    indices_master.drop(columns=['Company Name'], inplace=True) # think about this

    df = pd.merge(equity_l_df, indices_master, on=['Symbol', 'ISIN', 'Series'], how='left')
    df['NSE Index'] = df['NSE Index'].fillna('xxxxx')
    df.to_csv(os.path.join(PATH_1, 'symbols_master.csv'), index=False)
    print('Done, shape:', df.shape)
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

    print('CF_CA: Downloading %s ...' % from_to, end='')
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

def download_cf_shp(year):
    date_today = date.today()
    assert int(year) <= date_today.year, 'Invalid Year %s' % year
    if int(year) == date_today.year:
        from_to = ['01-01-%d' % int(year),'%s-%s-%d' % (('%d' % date_today.day).zfill(2),
                                                        ('%d' % date_today.month).zfill(2),
                                                        int(year))]
    else:
        from_to = ['01-01-%d' % int(year), '31-12-%d' % int(year)]
    url = 'https://www.nseindia.com/api/corporate-share-holdings-master?index=equities' + \
        '&from_date=%s&to_date=%s' % (from_to[0], from_to[1])

    print('CF_SHP: Downloading %s ...' % from_to, end='')
    http_obj = pyg_http_utils.HttpDownloads()
    cf_shp_json = http_obj.http_get_json(url)
    cf_shp_df = pd.DataFrame(cf_shp_json)
    cf_shp_df.reset_index(drop=True, inplace=True)
    cols = {
        'symbol': 'symbol',
        'name':'company_name',
        'recordId': 'recordId',
        'submissionDate': 'submissionDate',
        'date': 'date',
        'pr_and_prgrp': 'pr_and_prgrp',
        'public_val': 'public_val',
        'employeeTrusts': 'employeeTrusts',
        'xbrl': 'xbrl'
    }
    cf_shp_df.rename(columns=cols, inplace=True)
    cols_order = list(cols.values()) + [c for c in cf_shp_df.columns if c not in cols.values()]
    cf_shp_df = cf_shp_df[cols_order]
    cf_shp_df.to_csv(os.path.join(PATH_4, 'CF_SHP_%s.csv' % year), index=False)
    if len(cf_shp_json) != cf_shp_df.shape[0]:
        print('Warning! shapes are not matching. %d/%d' % (len(cf_shp_json), cf_shp_df.shape[0]))
    else:
        print('Done. cf_shp_df.shape:', cf_shp_df.shape)
    return

def last_n_pe_dates(n, last_period=None):
    end_date_str = date.today().strftime('%Y-%m-%d') if last_period is None else last_period
    pe_dates = []
    for yr in range(2018, datetime.strptime(end_date_str, '%Y-%m-%d').year + 1):
        for dt in ['%d-03-31' % yr, '%d-06-30' % yr, '%d-09-30' % yr, '%d-12-31' % yr]:
            if dt <= end_date_str:
                pe_dates.append(dt)
    return pe_dates[-n:]

def get_all(full=False, verbose=False):
    get_all_symbols()
    if full: get_all_indices()
    if full: get_etf_list()
    get_symbol_changes()
    if full: get_misc()
    if full: prepare_symbols_master()
    return True

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-y", type=int, nargs='+', help='calendar year')
    arg_parser.add_argument("-ca",  action='store_true', help='download cf_ca')
    arg_parser.add_argument("-shp", action='store_true', help='download cf_shp')
    arg_parser.add_argument("f", action='store_true', help='get full config set')
    args = arg_parser.parse_args()

    if args.y is None:
        get_all(full=args.f)
        print('Last 6 pe_dates:', last_n_pe_dates(6))
    else:
        print('Downloading CF_CA & CF_SHP for years:', args.y)
        if args.ca is False and args.shp is False:
            arg_parser.print_help()
            exit(0)
        for year in args.y:
            if args.ca:
                download_cf_ca(year)
            if args.shp:
                download_cf_shp(year)