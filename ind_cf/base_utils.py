"""
Common utils for xbrl retrieval and processing
TO DO
    1. Don't repeat if entry in S2 already
"""
''' --------------------------------------------------------------------------------------- '''
import glob
import os
import sys
import pandas as pd
import requests
import xml.etree.ElementTree as ElementTree
import pygeneric.http_utils as pyg_html_utils
import pygeneric.fin_utils as pyg_fin_utils

LOG_DIR = os.path.join(os.getenv('LOG_ROOT'), '01_fin_data/02_ind_cf')

''' --------------------------------------------------------------------------------------- '''
def get_xbrl(url):
    response = requests.get(url, headers=pyg_html_utils.http_request_header())
    if response.status_code != 200:
        raise ValueError('get_xbrl: code = %d, link = [%s]' % (response.status_code, url))
    return response.content

def load_filings_fr(path_regex):
    fr_filings_files = glob.glob(path_regex)
    print('\nbase_utils.load_filings_fr: Loading %d fr_filings files ... ' % len(fr_filings_files), end='')

    def rf(f):
        x = pd.read_csv(f)
        x['fileName'] = os.path.basename(f)
        return x

    fr_filings_df = pd.concat([rf(f) for f in fr_filings_files])
    print('Done. fr_filings_df.shape:', fr_filings_df.shape)

    empty_fr_xbrl_df = fr_filings_df.loc[fr_filings_df['xbrl'].str.endswith('/-')]
    empty_fr_xbrl_df.to_csv(os.path.join(LOG_DIR, 'empty_fr_xbrl_df.csv'), index=False)
    fr_filings_df.drop(empty_fr_xbrl_df.index, inplace=True)
    print('Dropped %d filings with empty fr xbrl links' % empty_fr_xbrl_df.shape[0])
    print('Net fr_filings_df.shape:', fr_filings_df.shape)
    fr_filings_df.reset_index(drop=True, inplace=True)

    print('fr_filings_df: shape: %s, %d unique XBRLs\n'
          % (fr_filings_df.shape, len(fr_filings_df['xbrl'].unique())))
    return fr_filings_df

def parse_xbrl_fr(xbrl_str):
    df = pd.DataFrame(columns=['tag', 'context'])
    root = ElementTree.fromstring(xbrl_str)
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
    balance_sheet = 'Present' \
        if df.loc[(df['context'] == 'OneI') &
                  (df['tag'].isin(['Assets', 'Liabilities', 'Equity']))].shape[0] >= 3 \
        else 'Absent'

    result = {
        'NSE Symbol': nse_symbol,
        'BSE Code': bse_code,
        'ISIN': ISIN,
        'period_start': period_start,
        'period_end': period_end,
        'fy_and_qtr': pyg_fin_utils.ind_fy_and_qtr(period_end),
        'reporting_qtr': df.loc[df['tag'] == 'ReportingQuarter', 'value'].values[0],
        'result_type': result_type,
        'result_format': result_format,
        'audited':df.loc[df['tag'] == 'WhetherResultsAreAuditedOrUnaudited', 'value'].values[0],
        'balance_sheet':balance_sheet,
        'company_name': company_name,
        'outcome': True,  # for now, later work on this
        'parsed_df': df
    }

    return result

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    urls = [
        'https://archives.nseindia.com/corporate/xbrl/INDAS_782747_4705_20102022204936_WEB.xml',
        'https://archives.nseindia.com/corporate/xbrl/NONINDAS_82817_609834_14022022021357_WEB.xml'
    ]

    for idx, url in enumerate(urls):
        xbrl_data = get_xbrl(url)
        assert len(xbrl_data) > 0, 'ERROR, empty xbrl_data'
        parsed_result = parse_xbrl_fr(xbrl_data)
        [print('%s: %s' % (k, parsed_result[k])) for k in parsed_result.keys() if k != 'parsed_df']
        parsed_result['parsed_df'].to_csv(
            os.path.join(LOG_DIR, f'parsed_df_{idx}.csv'), index=False)
        print('parsed_df saved, shape:', parsed_result['parsed_df'].shape)
        print()
    print('All OK')

    print('Testing load_filings_fr:::')
    f_df = load_filings_fr(
        os.path.join(os.getenv('DATA_ROOT'), f'01_fin_data/02_ind_cf/nse_fr_filings/CF_FR_*.csv'))
    print(f_df.columns)
    print(f_df['fileName'].unique())