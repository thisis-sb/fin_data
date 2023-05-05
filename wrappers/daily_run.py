"""
Daily run sequence
"""
''' --------------------------------------------------------------------------------------- '''

import datetime
from fin_data.common import nse_config
from fin_data.nse_pv import get_hpv, get_dr, process_dr
from fin_data.wrappers import test_nse_spot

''' --------------------------------------------------------------------------------------- '''
def daily_nse_pv():
    current_year = datetime.date.today().year

    print("\n>>> Daily task / common_nse_config::: Starting")
    nse_config.symbols_and_broad_indices()
    nse_config.sectoral_indices()
    nse_config.get_etf_list()
    nse_config.get_symbol_changes()
    nse_config.get_misc()
    nse_config.prepare_symbols_master()
    nse_config.get_cf_ca(current_year)
    print(">>> Daily task / nse_config::: Finished\n")

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

    print("\n>>> Daily task / test nse_pv.nse_spot::: Starting")
    if not test_nse_spot.test_all():
        print('ERROR! nse_spot.test_all FAILED')
        assert False, 'ERROR! nse_spot.test_all FAILED'
    print("\n>>> Daily task / test nse_pv.nse_spot::: Finished")

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    daily_nse_pv()