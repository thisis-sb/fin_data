"""
Scrape BSE for XBRL FR filings
"""
''' ----------------------------------------------------------------------------------------- '''

from fin_data.env import *
import datetime
import os
import sys
import time
import traceback
import logging
import pandas as pd
from selenium import webdriver
import selenium.webdriver.chrome.options as ChromeOptions
import selenium.webdriver.firefox.options as FirefoxOptions
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
import pygeneric.http_utils as pyg_http_utils
from fin_data.ind_cf import base_utils
import fin_data.common.nse_symbols as nse_symbols

DRIVER_TYPE = 'Firefox'  # 'Chrome'
PATH_1 = os.path.join(DATA_ROOT, '00_common')

''' ----------------------------------------------------------------------------------------- '''
class ScrapeBSE:
    def __init__(self, browser='Firefox', verbose=False):
        self.verbose = verbose
        self.browser = browser
        self.max_attempts = 5
        logging.disable(logging.INFO)
        self.url = 'https://www.bseindia.com/corporates/Comp_Resultsnew.aspx'
        self.driver = None
        df = pd.read_csv(os.path.join(PATH_1, '05_bse_symbols/EQ_ISINCODE.zip'))
        df = df[['ISIN_CODE', 'SC_CODE', 'SC_NAME', 'SC_GROUP', 'SC_TYPE']]
        df.rename(columns={'ISIN_CODE':'isin',
                           'SC_CODE':'bse_scrip_code',
                           'SC_NAME': 'bse_company_name',
                           'SC_GROUP': 'bse_group',
                           'SC_TYPE': 'bse_type'}, inplace=True)
        self.bse_codes = dict(zip(df['isin'], df.to_dict('records')))
        assert self.bse_codes[nse_symbols.get_isin('ASIANPAINT')]['bse_scrip_code'] == 500820
        assert self.bse_codes[nse_symbols.get_isin('SULA')]['bse_scrip_code'] == 543711
        if self.verbose:
            print('len(bse_codes):', len(self.bse_codes))

    def open_driver(self):
        if self.verbose:
            print('  Initializing driver ...', end=' ')

        if self.driver is not None:
            self.close_driver()

        if self.browser == 'Firefox':
            browser_options = FirefoxOptions.Options()
            browser_options.add_argument('--headless')
            self.driver = webdriver.Firefox(options=browser_options)
        else:
            browser_options = ChromeOptions.Options()
            browser_options.add_argument('--headless')
            browser_options.add_argument('log-level=1')
            browser_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            self.driver = webdriver.Chrome(options=browser_options)

        try:
            self.driver.get(self.url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.ID, 'ContentPlaceHolder1_trData')))
            if self.verbose:
                print('Done')
        except Exception as e:
            err_msg = 'open_driver failed: %s' % e
            self.close_driver()  # should it be here?
            raise ValueError(err_msg)
        return

    def close_driver(self):
        try:
            if self.verbose:
                print('Closing driver ...', end=' ')
            self.driver.quit()
            if self.verbose:
                print('Done')
            self.web_driver = None
        except Exception as e:
            if self.verbose:
                err_msg = 'close_driver failed: %s' % e
            self.web_driver = None  # should it be here?
            raise ValueError(err_msg)
        return

    def apply_query_filter(self, bse_scrip_code):
        self.open_driver()

        print('  Applying query filter:', end=' ')
        opt_1 = self.driver.find_element(By.ID, 'ContentPlaceHolder1_SmartSearch_smartSearch')
        opt_1.send_keys(f'{bse_scrip_code}' + Keys.ENTER)
        opt_2 = Select(self.driver.find_element(By.ID, 'ContentPlaceHolder1_broadcastdd'))
        opt_2.select_by_visible_text('Beyond last 1 year')
        opt_2.select_by_value('7')
        self.driver.find_element(By.ID, 'ContentPlaceHolder1_btnSubmit').click()
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_all_elements_located((By.ID, 'ContentPlaceHolder1_gvData'))
        )
        print('Done,', end=' ')

        table_id = self.driver.find_element(By.ID, 'ContentPlaceHolder1_tbComp')
        table_rows = table_id.find_elements(By.TAG_NAME, "tr")
        # print(len(table_rows), 'table rows,', end=' ')

        if len(table_rows) > 0:
            tr2 = table_rows[1].find_elements(By.TAG_NAME, "td")
            cell_values = tr2[1].find_elements(By.CLASS_NAME, 'tdcolumn')
            sec_code, sec_name = cell_values[0].text, cell_values[1].text
            if sec_code == f'{bse_scrip_code}':
                return {'outcome': True,
                        'n_rows': len(table_rows),
                        'n_cells': len(cell_values),
                        'msg_str': '%s / %s' % (sec_code, sec_name)
                        }
            else:
                return {'outcome': False,
                        'n_rows': 0,
                        'n_cells': 0,
                        'msg_str': 'apply_query_filter() failed: sec_code != bse_scrip_code'
                        }
        else:
            return {'outcome': False,
                    'n_rows': 0,
                    'n_cells': 0,
                    'msg_str': 'apply_query_filter() failed: len(table_rows) = 0'
                    }

    def scrape_fr(self, symbol):
        isin = nse_symbols.get_isin(symbol)
        if isin not in self.bse_codes.keys():
            return False, [], f'WARNING!!! scrape_fr: {symbol}/{isin}: isin lookup failed'
        bse_scrip_code = self.bse_codes[isin]['bse_scrip_code']
        if self.verbose:
            print(f'Scraping {symbol} / {bse_scrip_code}:::')

        href_links, xbrl_links = [], []
        try:
            filter_success, filter_result = False, {}
            n_attempts = 0
            while filter_success is not True and n_attempts < self.max_attempts:
                n_attempts = n_attempts + 1
                filter_result = self.apply_query_filter(bse_scrip_code)
                filter_success = filter_result['outcome']

            if filter_success and filter_result['n_rows'] > 0:
                print('%d rows, %d cells: filter applied (%d attempts)'
                      % (filter_result['n_rows'], filter_result['n_cells'], n_attempts))
            else:  # very strict? # if not filter_success or filter_result['n_rows'] == 0:
                raise RuntimeError('ERROR! Could not scrape data: [%s] (%d attempts)' %
                                   (filter_result['msg_str'], n_attempts))

            '''driver = filter_result['driver']  # this is important; error w/o it'''

            print('  Checking href_links:', end=' ')
            href_links = self.driver.find_elements(
                By.XPATH, "//*[contains(@id, 'ContentPlaceHolder1_gvData_lnkXML_')]"
            ) + self.driver.find_elements(
                By.XPATH, "//*[contains(@id, 'ContentPlaceHolder1_gvData_lnkConXML_')]"
            )
            print(len(href_links), 'href_links found,', end=' ')

            for hl in href_links:
                xx = '    x/y: %s' % hl.location_once_scrolled_into_view
                xx = '    x/y: %s' % hl.location_once_scrolled_into_view
                hl.click()
                WebDriverWait(self.driver, 10).until(EC.number_of_windows_to_be(2))
                self.driver.switch_to.window(self.driver.window_handles[1])
                WebDriverWait(self.driver, 10).until(EC.url_contains('bse'))
                xbrl_links.append(self.driver.current_url)
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[0])
            print(len(xbrl_links), 'xbrl_links found')
            assert len(href_links) == len(xbrl_links)
            return True, xbrl_links, None
        except TimeoutException:
            err_msg = 'ERROR! %s: scrape_xbrl_links failed (only %d/%d retrieved): TimeoutException\n\n%s' \
                      % (symbol, len(xbrl_links), len(href_links), traceback.format_exc())
            if self.verbose:
                print(err_msg)
            print(len(xbrl_links), 'xbrl_links found')
            self.close_driver()
            return False, xbrl_links, err_msg
        except Exception as e:
            err_msg = 'ERROR! %s: scrape_xbrl_links failed (only %d/%d retrieved): %s\n\n%s'\
                      % (symbol, len(xbrl_links), len(href_links), e, traceback.format_exc())
            if self.verbose:
                print(err_msg)
            print(len(xbrl_links), 'xbrl_links found')
            self.close_driver()
            return False, xbrl_links, err_msg

def test_me(symbols, verbose=False):
    assert type(symbols) == list, f'Invalid symbols type passed {type(symbols)}'
    bse_scrapper = ScrapeBSE(verbose=verbose)

    all_xbrl_links, error_list = [], []
    for symbol in symbols:
        print('\nscrape_bse.test_me: %s ...' % symbol)
        outcome, xbrl_links, err_msg = bse_scrapper.scrape_fr(symbol)
        if not outcome:
            error_list.append({'symbol':symbol,
                               'test_step': 'ScrapeBSE.scrape_fr',
                               'xbrl_link': None,
                               'error_msg':err_msg})
        if len(xbrl_links) > 0:
            all_xbrl_links = all_xbrl_links + xbrl_links

    if len(error_list) > 0:
        print('\n!!! %d ERRORS in ScrapeBSE.scrape_fr!!!\n' % len(error_list))
    else:
        print('\n\n!!! ScrapeBSE.scrape_fr ALL OK !!!\n')

    ''' NOTE (BSE brotli encoding) !!! https://requests.readthedocs.io/en/latest/community/faq/ '''
    print('Downloading & checking the scraped xbrl_links ...')
    http_obj = pyg_http_utils.HttpDownloads(website='bse')
    n_downloaded = 0
    for xbrl_link in all_xbrl_links:
        xbrl_data = http_obj.http_get(xbrl_link)
        if len(xbrl_data) == 0:
            error_list.append({'symbol': symbol,
                               'test_step': 'HttpDownloads.http_get',
                               'xbrl_link': xbrl_link,
                               'error_msg': 'http_get failed or empty xbrl_data'
                               })
            continue

        try:
            parsed_result = base_utils.parse_xbrl_fr(xbrl_data)
        except Exception as e:
            err_msg = 'corrputed xbrl_data:\n%s\n%s' % (e, traceback.format_exc())
            parsed_result = {'outcome': False}  # quick fix

        if not parsed_result['outcome']:
            error_list.append({'symbol': symbol,
                               'test_step': 'HttpDownloads.http_get',
                               'xbrl_link': xbrl_link,
                               'error_msg': err_msg
                               })
            continue
        n_downloaded += 1

    f = os.path.join(LOG_DIR, 'scrape_bse_errors.csv')
    if n_downloaded == len(all_xbrl_links) and len(error_list) == 0:
        if os.path.exists(f):
            os.remove(f)
        print('\n\n!!! SUCCESS: no errors. %d / %d xbrl_links downloaded !!!'
              % (n_downloaded, len(all_xbrl_links)))
    else:
        x = pd.DataFrame(error_list)
        x.to_csv(f, index=False)
        print('\n%d ERRORS. error_list saved in: %s' % (x.shape[0], f))

    return True

# --------------------------------------------------------------------------------------------
if __name__ == '__main__':
    test_symbols = ['PAYTM', 'ADANIGREEN', 'HDFCBANK', 'IDEAFORGE', 'SGIL'] if len(sys.argv) == 1 \
        else sys.argv[1:]
    test_me(symbols=test_symbols, verbose=False)