import os.path
import pandas as pd
import requests
import xml.etree.ElementTree as ElementTree
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import base.common
from global_env import CONFIG_DIR, DATA_ROOT, LOG_DIR

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


def fr_meta_data_files(exchange):
    META_DATA_DB = DATA_ROOT + f'/02_ind_cf/{exchange}_fr_metadata.csv'
    ERRORS_DB    = DATA_ROOT + f'/02_ind_cf/{exchange}_fr_errors.csv'
    return META_DATA_DB, ERRORS_DB


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
    elif df['tag'].str.contains('NameOfBank').any(): # dirty
        result_format = 'banking'
    else:
        result_format = 'default'

    if df['tag'].str.contains('NameOf').any():
        company_name = df.loc[df['tag'].str.startswith('NameOf'), 'value'].values[0]
    else:
        company_name = nse_symbol

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


def download_xbrl_fr(url, verbose=False):
    request_header = base.common.http_request_header()
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
            val = 'not-found'
        return val

    template_name = 'banking' if result_format == 'banking' else 'default'
    template_df = pd.read_excel(CONFIG_DIR + '/1_fr_templates.xlsx', sheet_name=template_name)

    template_df[period] = '  '
    for idx, row in template_df.iterrows():
        if row['attribute type'] == 'xbrl data':
            template_df.loc[idx, period] = get_value(row['xbrl context'], row['xbrl tag'])
        elif row['attribute type'] == 'LINE_SEP':
            template_df.loc[idx, period] = 'LINE_SEP'
    return template_df

# ---------------------------------------------------------------------------------------------
if __name__ == '__main__':
    url = 'https://www.bseindia.com/XBRLFILES/FourOneUploadDocument/' + \
          'Main_Ind_As_532921_2552022163752.xml'
    parsed_results = download_xbrl_fr(url)
    [print(k, parsed_results[k])
     for k in parsed_results.keys() if k != 'parsed_df' and k != 'xbrl_string']
    df = pre_fill_template(parsed_results['xbrl_string'])
    print(df.head().to_string(index=False))
