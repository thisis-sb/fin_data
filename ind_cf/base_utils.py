import os.path
import pandas as pd
import requests
import xml.etree.ElementTree as ElementTree
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utils
from module_settings import CONFIG_DIR, ROOT_DIR, LOG_DIR

# --------------------------------------------------------------------------------------------
def get_quarter(period_end):
    # YYYY-xxYY-Qx
    qtr_map = {'06':'Q1', '09':'Q2', '12':'Q3', '03':'Q4'}
    qtr = qtr_map[period_end[5:7]]
    if qtr == 'Q4':
        fy = '%d-%s' % (int(period_end[0:4])-1, period_end[0:4])
    else:
        fy = '%s-%d' % (period_end[0:4], int(period_end[0:4])+1)
    return '%s-%s-%s' % (fy[0:4], fy[7:], qtr)


def parse_fr_xml_string(xml_content_str):
    df = pd.DataFrame(columns=['tag', 'context'])
    root = ElementTree.fromstring(xml_content_str)
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

    if df['tag'].str.contains('ISIN').any():
        ISIN = df.loc[df['tag'] == 'ISIN', 'value'].values[0]
    else:
        ISIN = 'not-found'

    if df['tag'].str.contains('Symbol').any():
        nse_symbol = df.loc[df['tag'] == 'Symbol', 'value'].values[0]
    else:
        nse_symbol = 'not-found'

    if df['tag'].str.contains('ScripCode').any():
        bse_code = df.loc[df['tag'] == 'ScripCode', 'value'].values[0]
    else:
        bse_code = 'not-found'

    if df['tag'].str.contains('ResultType').any():
        result_format = df.loc[df['tag'] == 'ResultType', 'value'].values[0]
    elif df['tag'].str.contains('NameOfBank').any(): # dirty
        result_format = 'banking'
    else:
        result_format = 'default'

    if df['tag'].str.contains('NameOf').any():
        company_name = df.loc[df['tag'].str.startswith('NameOf'), 'value'].values[0]
    else:
        company_name = nse_symbol

    result_type = df.loc[df['tag'] == 'NatureOfReportStandaloneConsolidated', 'value'].values[0]

    if df['tag'].str.contains('DateOfStartOfReportingPeriod').any():
        period_start = df.loc[df['tag'] == 'DateOfStartOfReportingPeriod', 'value'].values[0]
    else:
        period_start = 'not-found'

    period_end = df.loc[df['tag'] == 'DateOfEndOfReportingPeriod', 'value'].values[0]

    result = {
        'outcome': True,
        'BSE Code': bse_code,
        'NSE Symbol': nse_symbol,
        'ISIN': ISIN,
        'period_start': period_start,
        'period_end': period_end,
        'period': get_quarter(period_end),
        'result_type': result_type,
        'result_format':result_format,
        'quarter_type': df.loc[df['tag'] == 'ReportingQuarter', 'value'].values[0],
        'company_name': company_name,
        'parsed_df': df
    }

    return result


def download_xbrl_fr(exchange, url, verbose=False):
    request_header = utils.http_request_header()
    # print('REQUEST HEADER:', request_header)

    response = requests.get(url, headers=request_header)
    if response.status_code != 200:
        raise ValueError('ERROR! download_xbrl_fr: code = %d, link = [%s]'
                         % (response.status_code, url))

    parsed_result = parse_fr_xml_string(response.content)
    if verbose and parsed_result['outcome']:
        parsed_result['parsed_df'].to_csv(
            LOG_DIR + '/parsed_xml_df_%s.csv' % parsed_result['NSE Symbol'], index=False)
    parsed_result['xbrl_string'] = response.content

    return parsed_result


def pre_fill_template(xbrl_content):
    parsed_result = parse_fr_xml_string(xbrl_content)
    # if OK ...
    period = parsed_result['period']
    result_format = parsed_result['result_format']

    raw_df = parsed_result['parsed_df']
    # raw_df.to_csv(LOG_DIR + '/raw_df.csv', index=False)

    def get_value(context, tag):
        try:
            val = raw_df.loc[(raw_df['context'] == context) &
                             (raw_df['tag'] == tag), 'value'].values[0]
        except Exception as e:
            val = 'not found'
        return val

    template_name = 'banking' if result_format == 'banking' else 'default'
    template_df = pd.read_excel(CONFIG_DIR + '/fin_data_fr/1_fr_templates.xlsx',
                                sheet_name=template_name)

    template_df[period] = '  '
    for idx, row in template_df.iterrows():
        if row['attribute type'] == 'xbrl data':
            template_df.loc[idx, period] = get_value(row['xbrl context'], row['xbrl tag'])
        elif row['attribute type'] == 'LINE_SEP':
            template_df.loc[idx, period] = 'LINE_SEP'
    return template_df

# ---------------------------------------------------------------------------------------------
if __name__ == '__main__':
    print('None for now')
