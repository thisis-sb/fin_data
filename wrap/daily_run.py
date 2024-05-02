"""
Daily run sequence
"""
''' --------------------------------------------------------------------------------------- '''

import sys
import datetime
from fin_data.common import nse_config, nse_symbols, nse_cf_ca
from fin_data.nse_pv import get_hpv, get_dr, process_dr
from fin_data.wrap import test_all
from fin_data.ind_cf import scrape_nse, download_fr

''' --------------------------------------------------------------------------------------- '''
def e2e_nse_common(full):
    print("\n>>> Daily task / e2e_nse_common::: Starting")
    print(120 * '-')
    current_year = datetime.date.today().year
    assert nse_config.get_all(full=full)
    nse_config.download_cf_ca(current_year)
    print(">>> Daily task / e2e_nse_common::: Finished\n")
    return

def e2e_nse_pv(common_as_well=True):
    print('\nRunning e2e_nse_pv ...')
    print(120 * '-')

    current_year = datetime.date.today().year

    print("\n>>> Daily task / nse_pv.get_hpv::: Starting")
    get_hpv.wrapper()
    print(">>> Daily task / nse_pv.get_hpv::: Finished\n")

    print("\n>>> Daily task / nse_pv.get_dr::: Starting")
    get_dr.nse_download_daily_reports(f'{current_year}',
                                      datetime.date.today().strftime("%b").upper())
    print(">>> Daily task / nse_pv.get_dr::: Finished\n")

    print("\n>>> Daily task / nse_pv.process_dr::: Starting")
    process_dr.wrapper(current_year)
    print(">>> Daily task / nse_pv.process_dr::: Finished\n")

    assert nse_symbols.test_me(), 'nse_symbols.test_me() failed'
    assert nse_cf_ca.test_me(), 'nse_cf_ca.test_me() failed'

    print("\n>>> Daily task / test nse_pv.nse_spot::: Starting")
    if not test_all.test_nse_spot():
        print('ERROR! nse_spot.test_all FAILED')
        assert False, 'ERROR! nse_spot.test_all FAILED'
    print("\n>>> Daily task / test nse_pv.nse_spot::: Finished")

    return

def e2e_ind_cf():
    print('\nRunning e2e_ind_cf ...')
    print(120 * '-')

    current_year = datetime.date.today().year

    print("\n>>> Task / scrape_nse: Starting")
    scrape_nse.get_nse_fr_filings(current_year)
    print("\n>>> Task / scrape_nse: Finished")

    print("\n>>> Task / download_fr: Starting")
    mgr = download_fr.DownloadManagerNSE(current_year)
    mgr.download()
    print("\n>>> Task / download_fr: Finished")

    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    opt = None if len(sys.argv) == 1 else sys.argv[1]

    if opt == 'nse_pv':
        e2e_nse_common(full=False)
        e2e_nse_pv()
    elif opt == 'nse_cf':
        e2e_nse_common(full=False)
        e2e_ind_cf()
    elif opt == 'all':
        nse_config.get_all(full=True)
        e2e_nse_pv()
        e2e_ind_cf()
    else:
        print('\nERROR! Bad options. Usage: nse_pv | nse_cf | all')