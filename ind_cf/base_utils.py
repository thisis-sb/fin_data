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
import pygeneric.fin_utils as pyg_fin_utils

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

def parse_xbrl_fr(xbrl_str, corrections=None):
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
        'https://archives.nseindia.com/corporate/xbrl/NONINDAS_82817_609834_14022022021357_WEB.xml',
        'https://nsearchives.nseindia.com/corporate/xbrl/INDAS_759407_5124_12102022203930_WEB.xml'
    ]

    for idx, url in enumerate(urls):
        xbrl_data = get_xbrl(url)
        assert len(xbrl_data) > 0, 'ERROR, empty xbrl_data'
        parsed_result = parse_xbrl_fr(xbrl_data)
        [print('%s: %s' % (k, parsed_result[k])) for k in parsed_result.keys() if k != 'parsed_df']
        parsed_result['parsed_df'].to_csv(
            os.path.join(LOG_DIR, 'parsed_df_%s_%s.csv' % (idx, parsed_result['NSE Symbol'])))
        print('parsed_df saved, shape:', parsed_result['parsed_df'].shape)
        print()

    ''' corrections '''
    url = 'https://nsearchives.nseindia.com/corporate/xbrl/INDAS_759407_5124_12102022203930_WEB.xml'
    corrs = [{'tag':'FaceValueOfEquityShareCapital', 'context':'OneD', 'value':'2'}]
    idx = 99  # meh
    xbrl_data = get_xbrl(url)
    assert len(xbrl_data) > 0, 'ERROR, empty xbrl_data'
    parsed_result = parse_xbrl_fr(xbrl_data, corrections=corrs)
    [print('%s: %s' % (k, parsed_result[k])) for k in parsed_result.keys() if k != 'parsed_df']
    parsed_result['parsed_df'].to_csv(
        os.path.join(LOG_DIR, 'parsed_df_%s_%s.csv' % (idx, parsed_result['NSE Symbol'])))
    print('parsed_df saved, shape:', parsed_result['parsed_df'].shape)
    print()

    print('All OK')