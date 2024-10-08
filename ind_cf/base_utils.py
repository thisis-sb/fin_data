"""
Base utils for CF downloads & processing
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import glob
import os
import sys
import pandas as pd
import requests
import xml.etree.ElementTree as ElementTree
from pygeneric.fin_utils import ind_fy_and_qtr

''' --------------------------------------------------------------------------------------- '''
def prepare_json_key(row):
    params = row['params']
    if '&' in row['symbol']:
        params = params.replace('&', '%26')
    seqNumber = row['seqNumber']
    industry = row['industry'] if pd.notna(row['industry']) else ''
    oldNewFlag = row['oldNewFlag'] if pd.notna(row['oldNewFlag']) else ''
    reInd = row['reInd']
    format_x = row['format']

    return 'params=%s&seq_id=%s' % (params, seqNumber) + \
           '&industry=%s&frOldNewFlag=%s' % (industry, oldNewFlag) + \
           '&ind=%s&format=%s' % (reInd, format_x)

def parse_xbrl_data(xbrl_data, xbrl_df=None, corrections=None):
    if xbrl_data is not None:  # xbrl_data has primacy
        df = pd.DataFrame(columns=['tag', 'context'])
        root = ElementTree.fromstring(xbrl_data)
        for item in root:
            if item.tag.startswith('{http://www.bseindia.com/xbrl/fin/'):
                tag = item.tag.split('}')[1]
                '''print(child.attrib['contextRef'])'''
                context = item.attrib['contextRef']  # this should always be there

                if tag.find('Disclosure') != -1: continue

                if not ((df['tag'] == tag) & (df['context'] == context)).any():  # add not already
                    df.loc[len(df.index)] = {'tag': tag, 'context': context}

                idx = df[(df['tag'] == tag) & (df['context'] == context)].index[0]
                df.at[idx, 'value'] = item.text
        df1 = df.copy()
    elif xbrl_data is None and xbrl_df is not None:
        df = xbrl_df.copy()
        df1 = None
    else:
        assert False, 'ERROR! base_utils.parse_xbrl_data: invalid combination'

    if corrections is not None:
        for corr in corrections:
            df.loc[(df['tag'] == corr['tag']) & (df['context'] == corr['context']),
                   'value'] = corr['value']

    def get_value(tag_value, value_if_not_found='not-found'):
        return df.loc[df['tag'] == tag_value, 'value'].values[0] \
            if df['tag'].str.contains(tag_value).any() else value_if_not_found

    ISIN         = get_value('ISIN')
    nse_symbol   = get_value('Symbol')
    bse_code     = get_value('ScripCode')
    period_start = get_value('DateOfStartOfReportingPeriod')
    period_end   = get_value('DateOfEndOfReportingPeriod')
    result_type  = get_value('NatureOfReportStandaloneConsolidated')

    if df['tag'].str.contains('ResultType').any():
        result_format = df.loc[df['tag'] == 'ResultType', 'value'].values[0]
        if result_format == 'Banking Format':
            result_format = 'banking'
        elif result_format == 'Main Format':
            result_format = 'default'
    elif df['tag'].str.contains('NameOfBank').any(): # dirty
        result_format = 'banking'
    else:
        result_format = 'default'

    if df['tag'].str.contains('NameOf').any():
        company_name = df.loc[df['tag'].str.startswith('NameOf'), 'value'].values[0]
    else:
        company_name = nse_symbol

    ''' check for balance sheet items '''
    balance_sheet = 'Absent'
    if result_format == 'default' and \
            df.loc[(df['context'] == 'OneI') &
                   (df['tag'].isin(['Assets', 'Liabilities', 'Equity', 'EquityAndLiabilities']))].shape[0] >= 3:
            balance_sheet = 'Present'
    elif result_format == 'banking' and \
            df.loc[(df['context'] == 'OneI') &
                   (df['tag'].isin(['Assets', 'CapitalAndLiabilities', 'Capital']))].shape[0] >= 3:
            balance_sheet = 'Present'

    result = {
        'NSE Symbol': nse_symbol,
        'BSE Code': bse_code,
        'ISIN': ISIN,
        'period_start': period_start,
        'period_end': period_end,
        'fy_and_qtr': ind_fy_and_qtr(period_end),
        'reporting_qtr': df.loc[df['tag'] == 'ReportingQuarter', 'value'].values[0],
        'result_type': result_type,
        'result_format': result_format,
        'audited':df.loc[df['tag'] == 'WhetherResultsAreAuditedOrUnaudited', 'value'].values[0],
        'balance_sheet':balance_sheet,
        'company_name': company_name,
        'outcome': True,  # for now, later work on this
        'xbrl_df': df1,
        'parsed_df': df
    }

    return result

def test_me(verbose=False):
    print('\nfin_data.ind_cf.base_utils.test_me:', end=' ')
    import json
    from pygeneric import archiver_cache
    from pygeneric.datetime_utils import elapsed_time

    test_data = pd.read_excel(os.path.join(CONFIG_ROOT, '03_fin_data.xlsx'), sheet_name='test_data')
    test_data = test_data.loc[test_data['module'] == 'ind_cf.base_utils'].\
        dropna(subset=['module', 'symbol']).reset_index(drop=True).to_dict('records')

    PATH_1 = os.path.join(DATA_ROOT, '02_ind_cf/02_nse_fr_archive')
    md_files = glob.glob(os.path.join(PATH_1, 'metadata_*.csv'))
    meta_data = pd.concat([pd.read_csv(f) for f in md_files])
    def xbrl_archive_path_func(xbrl_key):
        _xx = meta_data.loc[meta_data['xbrl_key'] == xbrl_key, 'xbrl_archive_path']
        return None if _xx.shape[0] == 0 else os.path.join(PATH_1, _xx.values[0])
    ac_xbrl = archiver_cache.ArchiverCache(xbrl_archive_path_func, cache_size=5)

    elapsed_time('fin_data.ind_cf.base_utils.test_me.0')
    for tc in test_data:
        tc_inp = json.loads(tc['test_input'])
        xbrl_data = ac_xbrl.get_value(tc_inp['xbrl_key'])
        pr1 = parse_xbrl_data(xbrl_data=xbrl_data, xbrl_df=None, corrections=None)
        parsed_df = pr1['parsed_df']
        xbrl_df   = pr1['xbrl_df']
        pr1.pop('parsed_df')
        pr1.pop('xbrl_df')
        pr2 = parse_xbrl_data(xbrl_data=None, xbrl_df=xbrl_df, corrections=None)
        assert parsed_df.equals(pr2['parsed_df'])
        assert pr2['xbrl_df'] is None
        pr2.pop('parsed_df')
        pr2.pop('xbrl_df')
        assert pr1 == pr2

    t = elapsed_time('fin_data.ind_cf.base_utils.test_me.0')
    print('OK (%.2f)' % t)

    return True, t

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    verbose = True
    test_me(verbose=verbose)