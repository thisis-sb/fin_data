"""
Daily run sequence
"""
''' --------------------------------------------------------------------------------------- '''

import sys
import datetime
from fin_data.common import nse_config, nse_symbols, nse_cf_ca
from fin_data.nse_pv import get_hpv, get_dr, process_dr
from fin_data.ind_cf import base_utils, scrape_nse, download_fr, process_fr
from fin_data.apps import test_all

''' --------------------------------------------------------------------------------------- '''
def e2e_nse_common(full):
    print("\nfin_data.apps.daily_run.e2e_nse_common:")
    print(100 * '-')
    current_year = datetime.date.today().year
    assert nse_config.get_all(full=full)
    nse_config.download_cf_ca(current_year)
    print("fin_data.apps.daily_run.e2e_nse_common: COMPLETE")
    print(100 * '-')
    return

def e2e_nse_pv(common_as_well=True, run_tests=True):
    print('\n\nfin_data.apps.daily_rune2e_nse_pv:')
    print(100 * '-')

    current_year = datetime.date.today().year

    print("\nfin_data.nse_pv.get_dr:")
    print(100 * '-')
    get_dr.nse_download_daily_reports(f'{current_year}', datetime.date.today().strftime("%b").upper())
    print("fin_data.nse_pv.get_dr: COMPLETE")

    print("\n\nfin_data.nse_pv.process_dr:")
    print(100 * '-')
    process_dr.wrapper(current_year)
    print("fin_data.nse_pv..process_dr: COMPLETE")

    print("\n\nfin_data.nse_pv.get_hpv:")
    print(100 * '-')
    get_hpv.wrapper()
    print("fin_data.nse_pv.get_hpv: COMPLETE")

    if run_tests:
        print("\n\nTESTING nse_pv:")
        print(100 * '-')
        assert nse_symbols.test_me(), 'nse_symbols.test_me() failed'
        assert nse_cf_ca.test_me(), 'nse_cf_ca.test_me() failed'
        r, t = test_all.test_nse_spot()
        assert r, 'ERROR! nse_spot.test_all FAILED'
        print('test_all.test_nse_spot: time: %.1f sec' % t)

        print("fin_data.apps.daily_run.e2e_nse_pv: COMPLETE.")
        print(100 * '-')

    return

def e2e_ind_cf(run_tests=True):
    print('\n\nfin_data.apps.daily_run.e2e_ind_cf:')
    print(100 * '-')

    current_year = datetime.date.today().year

    print("\nfin_data.ind_cf.get_nse_fr_filings:")
    print(100 * '-')
    scrape_nse.get_nse_fr_filings(current_year)
    print("fin_data.ind_cf.get_nse_fr_filings: COMPLETE")

    print("\n\nfin_data.ind_cf.download_fr:")
    print(100 * '-')
    download_fr.DownloadManagerNSE(current_year).download()
    print("fin_data.ind_cf.download_fr:")

    print("\n\nfin_data.ind_cf.process_fr:")
    process_fr.ProcessCFFRs(current_year).process()
    print("fin_data.ind_cf.process_fr: COMPLETE")

    if run_tests:
        print("\n\nTESTING ind_cf:")
        print(100 * '-', end='')
        r, t = base_utils.test_me()
        assert r, 'ERROR! base_utils.test_me FAILED'

    print("fin_data.apps.daily_run.e2e_ind_cf: COMPLETE.")
    print(100 * '-')

    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    opt = None if len(sys.argv) == 1 else sys.argv[1]

    if opt == 'nse_pv':
        e2e_nse_common(full=False)
        e2e_nse_pv()
    elif opt == 'ind_cf':
        e2e_nse_common(full=False)
        e2e_ind_cf()
    elif opt == 'all':
        nse_config.get_all(full=True)
        e2e_nse_pv()
        e2e_ind_cf()
    else:
        print('\nERROR! Bad options. Usage: nse_pv | ind_cf | all')