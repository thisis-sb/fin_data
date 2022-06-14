# --------------------------------------------------------------------------------------------
# Scrape XBRL FR links from BSE
# Usage: nse_symbols_file n_to_download
# --------------------------------------------------------------------------------------------

import datetime
import os
import sys
import time
import pandas as pd
from selenium import webdriver
import selenium.webdriver.chrome.options as ChromeOptions
import selenium.webdriver.firefox.options as FirefoxOptions
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import traceback
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import api.nse_symbols
from settings import DATA_ROOT, CONFIG_DIR, LOG_DIR

# DRIVER_TYPE = 'Firefox'
DRIVER_TYPE = 'Chrome'

def close_driver(driver, verbose=False):
    if verbose: print('Closing driver ...', end=' ')
    driver.quit()
    if verbose: print('Done')
    driver = None
    return driver

def init_driver(driver):
    print('Initializing driver ...', end=' ')
    url = 'https://www.bseindia.com/corporates/Comp_Resultsnew.aspx'
    if driver is not None: close_driver(driver)
    if DRIVER_TYPE == 'Firefox':
        browser_options = FirefoxOptions.Options()
        browser_options.add_argument('--headless')
        driver = webdriver.Firefox(options=browser_options)
    else:
        browser_options = ChromeOptions.Options()
        browser_options.add_argument('--headless')
        browser_options.add_argument('log-level=1')
        browser_options.add_experimental_option('excludeSwitches', ['enable-logging'])
        driver = webdriver.Chrome(options=browser_options)

    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.ID, 'ContentPlaceHolder1_trData')))
        table_rows = driver.find_elements(By.XPATH, "//*[@class= 'mGrid']/tbody/tr")
        print(f'Done ({len(table_rows)})')
    except Exception as e:
        err_msg = f'ERROR! init_driver failed: {e}, [{traceback.format_exc()}]'
        close_driver(driver, True)
        raise ValueError(err_msg)
    return driver

def scrape_xbrl_links(security_code, n_rows, driver):
    print(f'Scraping {security_code}:', end=' ')

    def apply_query_filter(wd, security_code):
        wd = init_driver(wd)
        opt_scrip = wd.find_element(By.ID, 'ContentPlaceHolder1_SmartSearch_smartSearch')
        opt_scrip.send_keys(f'{security_code}' + Keys.ENTER)
        opt_period = Select(wd.find_element(By.ID, 'ContentPlaceHolder1_broadcastdd'))
        opt_period.select_by_visible_text('Beyond last 1 year')
        opt_period.select_by_value('7')
        wd.find_element(By.ID, 'ContentPlaceHolder1_btnSubmit').click()
        WebDriverWait(wd, 10).until(EC.presence_of_all_elements_located((By.ID, 'ContentPlaceHolder1_gvData')))

        table_id = wd.find_element(By.ID, 'ContentPlaceHolder1_tbComp')
        table_rows = table_id.find_elements(By.TAG_NAME, "tr")
        print(len(table_rows), 'table rows found,', end=' ')

        if len(table_rows) > 1:
            tr2 = table_rows[1].find_elements(By.TAG_NAME, "td")
            cell_values = tr2[1].find_elements(By.CLASS_NAME, 'tdcolumn')
            # print(len(cell_values), 'cell values found', end=' ')
            sec_code, sec_name = cell_values[0].text, cell_values[1].text
            # print('sec_code, sec_name:', sec_code, sec_name)
            # print('Closing driver');driver.quit();exit()
            if sec_code == f'{security_code}':
                return {
                    'outcome':True, 'driver':wd,
                    'n_rows':len(table_rows),
                    'n_cells':len(cell_values),
                    'msg_str':'%s / %s' % (sec_code, sec_name)
                }
        # ----------------------------------------------------------------------------
        print('apply_query_filter() failed', end=' ')
        return {
            'outcome': False, 'driver': wd,
            'n_rows': 0, 'n_cells': 0, 'msg_str': 'apply_query_filter() failed'
        }

    try:
        filter_success, filter_result = False, {}
        n_attempts = 0
        while filter_success is not True and n_attempts < 5:
            n_attempts = n_attempts + 1
            filter_result = apply_query_filter(driver, security_code)
            filter_success = filter_result['outcome']

        if filter_success and filter_result['n_rows'] > 0:
            print('%d cells --> %s: filter applied (in %d attempts)'
                  % (filter_result['n_cells'], filter_result['msg_str'], n_attempts))
        else:  # very strict? # if not filter_success or filter_result['n_rows'] == 0:
            raise RuntimeError('ERROR! Could not scrape data: [%s] (%d attempts)' %
                               (filter_result['msg_str'], n_attempts))

        driver = filter_result['driver']  # this is important; error w/o it

        # later: min(n_rows, tc_rows)
        print('Identifying xbrl links,', end=' ')
        href_links = driver.find_elements(
            By.XPATH, "//*[contains(@id, 'ContentPlaceHolder1_gvData_lnkXML')]")
        print(len(href_links), 'XBRL links ...', end=' ')
        # print('\nClosing driver');driver.quit();exit()

        xbrl_urls = []
        for hl in href_links:
            hl.click()
            WebDriverWait(driver, 10).until(EC.number_of_windows_to_be(2))
            driver.switch_to.window(driver.window_handles[1])
            xbrl_urls.append(driver.current_url)
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
        print(len(xbrl_urls), 'XBRL links identified')
        assert len(href_links) == len(xbrl_urls)
        return True, xbrl_urls, None
    except Exception as e:
        x1 = traceback.extract_tb(e.__traceback__).format()
        x2 = x1[-1].split('\n')[-2]
        err = f'ERROR! scrape_xbrl_links failed: {e} {x2}', traceback.format_exc()
        print(err[0])
        close_driver(driver, True)
        return False, None, err

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    nse_symbols_file = 'ind_nifty500list' if len(sys.argv) == 1 else sys.argv[1]
    n_to_download = 101 if len(sys.argv) <= 2 else int(sys.argv[2])
    print('nse_symbols_file:', nse_symbols_file, 'n_to_download:', n_to_download)

    # nse_symbols = ['3MINDIA', 'ASIANPAINT', 'SBILIFE', 'ICICIBANK', 'TATASTEEL', 'HDFCLIFE']
    index_files = [nse_symbols_file]
    nse_symbols = api.nse_symbols.get_symbols(index_files)

    nse_eq_df = pd.read_csv(CONFIG_DIR + '/2_nse_symbols/EQUITY_L.csv')
    nse_eq_df.rename(columns={'SYMBOL':'NSE Symbol', 'NAME OF COMPANY':'COMPANY NAME',
                              ' SERIES':'SERIES', ' ISIN NUMBER':'ISIN'}, inplace=True)
    nse_eq_df = nse_eq_df[['NSE Symbol', 'ISIN', 'SERIES', 'COMPANY NAME']].loc[
        nse_eq_df['NSE Symbol'].isin(nse_symbols)].reset_index(drop=True)

    bse_eq_df = pd.read_csv(CONFIG_DIR + '/1_bse_codes/bse_codes.csv', index_col=False)
    bse_eq_df.rename(columns={'Security Code':'BSE Code', 'ISIN No':'ISIN'}, inplace=True)
    bse_eq_df['BSE Code'] = bse_eq_df['BSE Code'].astype(int)
    bse_eq_df = bse_eq_df[['BSE Code', 'ISIN', 'Sector Name', 'Industry', 'Group']]
    # bse_eq_df.to_csv(LOG_DIR + '/ind_cf/bse_eq_df.csv', index=False); exit()

    nse_eq_df = pd.merge(nse_eq_df, bse_eq_df, on='ISIN', how='left').reset_index(drop=True)
    nse_eq_df = nse_eq_df[['NSE Symbol', 'BSE Code', 'ISIN', 'SERIES', 'COMPANY NAME',
                           'Group', 'Sector Name', 'Industry']]
    nse_eq_df['BSE Code'] = nse_eq_df['BSE Code'].fillna(0)
    nse_eq_df['BSE Code'] = nse_eq_df['BSE Code'].astype(int)
    nse_eq_df.to_csv(LOG_DIR + '/ind_cf/nse_eq_df.csv', index=False)  # ;exit()

    logging.disable(logging.INFO)
    web_driver = init_driver(None)
    print('Initial setup OK\n')

    # Test one
    # successful, xbrl_links, msg_str = scrape_xbrl_links(500820, 4, driver)
    # print('scrape_xbrl_links outcome:', successful, len(xbrl_links))
    # driver.quit(); exit()

    scrape_timestamp = datetime.datetime.today().strftime('%Y-%m-%d-%H-%M')
    f1 = f'/02_ind_cf/bse_fr_1/scrape_results_{scrape_timestamp}.csv'
    f2 = f'/02_ind_cf/bse_fr_1/scrape_log.csv'
    scrape_log = pd.read_csv(DATA_ROOT + f2) if os.path.exists(DATA_ROOT + f2) else pd.DataFrame()
    print('Pre-existing scraped_log.shape:', scrape_log.shape)

    scrape_results, n_success, n_errors, n_skipped = pd.DataFrame(), 0, 0, 0

    def save_scrape_results():
        if scrape_results.shape[0] > 0:
            scrape_results.reset_index(drop=True, inplace=True)
            scrape_results.to_csv(DATA_ROOT + f1, index=False)
        if scrape_log.shape[0] > 0:
            scrape_log.reset_index(drop=True, inplace=True)
            scrape_log.to_csv(DATA_ROOT + f2, index=False)
        print('\n>>> ---------- scrape results & log data saved ---------- <<<\n')
        return

    '''
    1. only put successful results in scrape_results
    2. success & error outcome in scrape_log
    '''
    for idx, row in nse_eq_df.iterrows():
        if n_success > n_to_download:
            print(f'Limit reached {n_success}/{n_to_download}. Stopping here')
            break

        if idx % 10 == 0 and (n_success + n_errors) > 0:  # optimize it further later
            save_scrape_results()

        print('\n%d / %d (%s)::' % (idx+1, nse_eq_df.shape[0], row['NSE Symbol']), end=' ')

        if scrape_log.shape[0] > 0 and row['NSE Symbol'] in scrape_log['NSE Symbol'].values:
            print('symbol exists in scrape log (xx links). Skipping\n')
            n_skipped += 1
            continue

        successful, xbrl_links, err_msg_tuple = scrape_xbrl_links(row['BSE Code'], 4, web_driver)
        if not successful:
            scrape_log = pd.concat([scrape_log, pd.DataFrame(
                {'NSE Symbol': row['NSE Symbol'], 'BSE Code': row['BSE Code'],
                 'ISIN': row['ISIN'], 'n_XBRL_Links': 0, 'timestamp':scrape_timestamp,
                 'error_msg': err_msg_tuple[0], 'traceback': err_msg_tuple[1]
                 }, index=[0])])
            n_errors += 1
        elif len(xbrl_links) == 0:
            scrape_log = pd.concat([scrape_log, pd.DataFrame(
                {'NSE Symbol': row['NSE Symbol'], 'BSE Code': row['BSE Code'],
                 'ISIN': row['ISIN'], 'n_XBRL_Links': 0, 'timestamp':scrape_timestamp,
                 'error_msg': 'No XBRL links found', 'traceback': err_msg_tuple
                 }, index=[0])])
            n_errors += 1
        else:
            scrape_results = pd.concat([scrape_results, pd.DataFrame(
                {'NSE Symbol':[row['NSE Symbol'] for i in range(len(xbrl_links))],
                 'BSE Code': [row['BSE Code'] for i in range(len(xbrl_links))],
                 'ISIN': [row['ISIN'] for i in range(len(xbrl_links))],
                 'XBRL Link': xbrl_links
                 }, index=range(len(xbrl_links)))])

            scrape_log = pd.concat([scrape_log, pd.DataFrame(
                {'NSE Symbol': row['NSE Symbol'], 'BSE Code': row['BSE Code'],
                 'ISIN': row['ISIN'], 'n_XBRL_Links': len(xbrl_links), 'timestamp':scrape_timestamp,
                 'error_msg': None, 'traceback': None
                 }, index=[0])])
            n_success += 1
        time.sleep(1)
    # for loop scrapping completed ----------------------------------------------------------

    # final save
    save_scrape_results()
    close_driver(web_driver)

    print('Summary: %d symbols: %d successful, %d skipped, %d had errors'
          % (nse_eq_df.shape[0], n_success, n_skipped, n_errors))