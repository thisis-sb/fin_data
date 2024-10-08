"""
test all: nse_symbols, nse_spot
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import random
from datetime import datetime, timedelta
import pandas as pd
import fin_data.nse_pv.nse_spot as nse_spot
from fin_data.common import nse_config, nse_symbols, nse_cf_ca
from fin_data.nse_pv import get_hpv, get_dr, process_dr, nse_spot
from fin_data.ind_cf import base_utils
from pygeneric.datetime_utils import elapsed_time, remove_timers

''' --------------------------------------------------------------------------------------- '''
def test_nse_spot(verbose=False):
    print('\nfin_data.apps.test_all.test_nse_spot:')
    print(80 * '-')

    elapsed_time('test_nse_spot_0')
    import fin_data.nse_pv.get_hpv as get_hpv

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData basics:')
    symbols = ['ASIANPAINT', 'BRITANNIA', 'HDFC', 'HDFCBANK', 'ICICIBANK', 'IRCON', 'IRCTC',
               'JUBLFOOD', 'TATASTEEL', 'ZYDUSLIFE']
    nse_spot_obj = nse_spot.NseSpotPVData(verbose=False)

    def compare_dfs(symbol, df1, df2):
        df1.to_csv(os.path.join(LOG_DIR, 'nse_spot_df1.csv'), index=False)
        df2.to_csv(os.path.join(LOG_DIR, 'nse_spot_df2.csv'), index=False)
        assert abs(df1.shape[0] - df2.shape[0]) <= 5, "%d / %d" % (df1.shape[0], df2.shape[0])

        if df1.shape[0] != df2.shape[0]:
            common_dates = list(set(df1['Date']) & set(df2['Date']))
            df1 = df1.loc[df1['Date'].isin(common_dates)].reset_index(drop=True)
            df2 = df2.loc[df2['Date'].isin(common_dates)].reset_index(drop=True)

        for c in ['Open', 'High', 'Low', 'Close', 'Prev Close', 'Volume']:
            assert (~(abs(df1[c] - df2[c]) < 0.5)).sum() == 0, \
                '\nERROR!! verify_results/1: %s: Column: %s: %d'\
                % (symbol, c, (~(abs(df1[c] - df2[c]) < 0.5)).sum())

        for c in ['Delivery Volume']:
            if (~(abs(df1[c].astype(float) - df2[c].replace('-', 0).astype(float)) < 0.5)).sum() > 1:
                print('\nWARNING!! verify_results/2: %s: Column: %s: %d'
                      % (symbol, c, (~(abs(df1[c].astype(float) - df2[c].replace('-', 0).astype(float)) < 0.5)).sum()))

        for c in ['No Of Trades', 'Traded Value']:
            if (~(abs(df1[c] - df2[c]) < 5)).sum() > 1:
                print('\nWARNING!! verify_results/3: %s: Column: %s: %d'
                      % (symbol, c, (~(abs(df1[c] - df2[c]) < 0.5)).sum()))

        return

    def check_data(symbol_list, dates):
        print(f'  Checking for {dates}:', dates, end=' ')
        for symbol in symbol_list:
            if verbose: print(f'\n  {symbol} ...', end=' ')
            df1 = nse_spot_obj.get_pv_data(symbol, series='EQ', from_to=dates)
            df2 = get_hpv.get_pv_data(symbol, from_to=dates)
            compare_dfs(symbol, df1, df2)
            if verbose: print('OK', end='')
        print('') if verbose else print('OK')
        return

    end_date = (datetime.today() - timedelta(days=2)).strftime('%Y-%m-%d')
    check_data(symbols, ['2018-01-01', '2018-12-31'])
    check_data(symbols, ['2019-01-01', '2019-12-31'])
    check_data(symbols, ['2020-01-01', '2020-12-31'])
    check_data(symbols, ['2021-01-01', '2021-12-31'])
    check_data(symbols, ['2022-01-01', '2022-12-31'])
    check_data(symbols, ['2023-01-01', '2023-12-31'])
    check_data(symbols, ['2024-01-01', end_date])

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData.get_pv_data (for multiple symbols):', end=' ')
    """ To do: Verify 52_Wk_H/L"""
    multi_df = nse_spot_obj.get_pv_data(symbols, from_to=['2018-01-01', end_date])
    for symbol in symbols:
        if verbose: print(f'\n  {symbol} ...', end=' ')
        df1 = multi_df.loc[multi_df['Symbol'] == symbol].reset_index(drop=True)
        df2 = get_hpv.get_pv_data(symbol, from_to=['2018-01-01', end_date])
        compare_dfs(symbol, df1, df2)
        if verbose: print('OK', end='')
    print('') if verbose else print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData.get_index_pv_data:', end=' ')
    pv_data = nse_spot_obj.get_index_pv_data('NIFTY 50', ['2023-04-01', '2023-05-02'])
    assert pv_data.shape[0] == 18 and pv_data.shape[1] == 13
    pv_data = nse_spot_obj.get_index_pv_data('NIFTY 50', ['2023-04-01', None])
    assert pv_data.shape[0] > 0 and pv_data.shape[1] == 13
    pv_data = nse_spot_obj.get_index_pv_data(['NIFTY 50', 'NIFTY MIDCAP 150', 'NIFTY IT'], ['2023-04-01', '2023-05-02'])
    assert pv_data.shape[0] == 54 and pv_data.shape[1] == 13
    pv_data = nse_spot_obj.get_index_pv_data('NIFTY 50', ['2010-01-01', '2019-12-31'])
    assert pv_data.shape[0] == 2443 and pv_data.shape[1] == 13
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData.get_etf_pv_data:', end=' ')
    pv_data = nse_spot_obj.get_etf_pv_data('NIFTYBEES', ['2023-04-01', '2023-05-02'])
    assert pv_data.shape[0] == 18 and pv_data.shape[1] == 17, pv_data.shape
    pv_data = nse_spot_obj.get_etf_pv_data('NIFTYBEES', ['2023-04-01', None])
    assert pv_data.shape[0] > 0 and pv_data.shape[1] == 17, pv_data.shape
    pv_data = nse_spot_obj.get_etf_pv_data(['NIFTYBEES', 'ITBEES', 'CPSEETF'], ['2023-04-01', '2023-05-02'])
    assert pv_data.shape[0] == 54 and pv_data.shape[1] == 17, pv_data.shape
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData.get_spot_quote:', end=' ')
    keys  = ['Symbol', 'Series', 'Date', 'epoch', 'Open', 'High', 'Low', 'Close',
             'previousClose', 'lastPrice', 'pChange']
    assert list(nse_spot.get_spot_quote('ASIANPAINT').keys()) == keys
    assert list(nse_spot.get_spot_quote('NIFTY 50', index=True).keys()) == keys
    res = nse_spot.get_spot_quote(['NIFTY IT', 'NIFTY MIDCAP 150', 'NIFTY AUTO'], index=True)
    assert type(res) == list and len(res) == 3 and list(res[2].keys()) == keys
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData.get_pv_data (for partly paid symbol):.', end=' ')
    df_pp = nse_spot_obj.get_pv_data('AIRTELPP', series='E1', from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    df_pp = nse_spot_obj.get_pv_data(['BHARTIARTL', 'AIRTELPP'], from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    df_pp = nse_spot_obj.get_pv_data(['BRITANNIA'], from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData.get_pv_data (for large # of multiple symbols):')
    elapsed_time(0)
    import fin_data.common.nse_symbols as nse_symbols
    symbols = nse_symbols.get_symbols(['NIFTY 50', 'NIFTY NEXT 50'])
    df = nse_spot_obj.get_pv_data(symbols, from_to=['2018-01-01', None], verbose=True)
    print('OK. time check:', elapsed_time(0), 'seconds\n')

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData.get_pv_data (for same symbol, different series):', end=' ')
    ''' TO DO: Need a better solution (for current ones & not discontinued)'''
    assert nse_spot_obj.get_pv_data('HDFC', series='EQ', from_to=['2023-05-12', None])['Close'].values[0] == 2776.3
    assert nse_spot_obj.get_pv_data('HDFC', series='W3', from_to=['2023-05-12', None])['Close'].values[0] == 560.55
    assert nse_spot_obj.get_pv_data('BRITANNIA', series='EQ', from_to=['2023-05-12', None])['Close'].values[0] == 4616.50
    assert nse_spot_obj.get_pv_data('BRITANNIA', series='N3', from_to=['2023-05-12', None])['Close'].values[0] == 29.54
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('NseSpotPVData.get_avg_closing_price:', end=' ')
    res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2021-12-31')
    assert abs(res[2] - 3425.62) < 0.1, 'ERROR! %s Unexpected average value' % res
    res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2022-12-31')
    assert abs(res[2] - 3057.05) < 0.1, 'ERROR! %s Unexpected average value' % res
    res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2023-03-31')
    assert abs(res[2] - 2784.44) < 0.1, 'ERROR! %s Unexpected average value' % res
    print('OK')

    t = elapsed_time('test_nse_spot_0')
    print('\nNseSpotPVData tests total time: %.2f' % t)
    print(80 * '-')

    return True, t

def test_perf_nse_pv(verbose=False):
    print('\nfin_data.apps.test_me.test_perf_nse_pv:')
    print(80 * '-')

    elapsed_time(['tpnp_0', 'tpnp_1'])
    nse_spot_obj = nse_spot.NseSpotPVData(verbose=verbose)
    print('Step1: initialization: %.2f' % elapsed_time('tpnp_1'))

    dates_from = [(datetime.today()-timedelta(1*365)).strftime('%Y-%m-%d'),
                  (datetime.today()-timedelta(3*365)).strftime('%Y-%m-%d')]

    for dt in dates_from:
        symbols = nse_symbols.get_symbols(['NIFTY 50'])
        print('\nStep2 for date:', dt, '...')
        for i in range(3):
            one_symbol = random.choice(symbols)
            elapsed_time('tpnp_1')
            df = nse_spot_obj.get_pv_data(one_symbol, from_to=[dt, None], verbose=verbose)
            print('  get_pv_data: %s: %d rows (%.2f sec)'
                  % (one_symbol, df.shape[0], elapsed_time('tpnp_1')))

        print('Step3 for date:', dt, '...')
        for i in range(3):
            few_symbols = random.sample(symbols, (i + 1) * 10)
            elapsed_time('tpnp_1')
            df = nse_spot_obj.get_pv_data(few_symbols, from_to=[dt, None], verbose=verbose)
            print('  get_pv_data: %d symbols: %d rows (%.2f sec)'
                  % (len(few_symbols), df.shape[0], elapsed_time('tpnp_1')))

        elapsed_time('tpnp_1')
        print('Step4 for date:', dt, '...')
        for ix in ['NIFTY 50', 'NIFTY 500', 'NIFTY TOTAL MARKET']:
            symbols = nse_symbols.get_symbols([ix])
            df = nse_spot_obj.get_pv_data(symbols, from_to=[dt, None], verbose=verbose)
            print('  get_pv_data: %s: %d rows (%.2f sec)'
                  % (ix, df.shape[0], elapsed_time('tpnp_1')))

    print('\nStep5: Testing 50 calls to get_avg_closing_price:', end=' ')
    symbols = nse_symbols.get_symbols(['NIFTY 100'], series='EQ')
    elapsed_time('tpnp_1')
    select_symbols = random.sample(symbols, 50)
    for s in select_symbols:
        try:
            _ = nse_spot_obj.get_avg_closing_price(s, mid_point='2024-01-15')
        except Exception as e:
            print('\n    %s: ERROR: %s' % (s, e))
    print('time: %.2f sec' % elapsed_time('tpnp_1'))

    t = elapsed_time('tpnp_0')
    print('test_nse_spot: total time taken: %.2f' % t)
    print(80 * '-')
    remove_timers(['tpnp_0', 'tpnp_1'])

    return True, t

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    assert nse_symbols.test_me(), 'nse_symbols.test_me() failed'
    assert nse_cf_ca.test_me(), 'nse_cf_ca.test_me() failed'

    test_outcomes = {}
    test_outcomes['test_nse_spot'] = test_nse_spot()
    test_outcomes['test_perf_nse_pv'] = test_perf_nse_pv()
    test_outcomes['base_utils'] = base_utils.test_me()

    print('\nfin_data.test_all outcome:\n%s' % (40 * '-'))
    for k in test_outcomes.keys():
        assert test_outcomes[k][0]
        print('%-20s %5.1f s' % (k, test_outcomes[k][1]))
    print('%-20s %5.1f s' % ('total time', sum([t[1] for t in test_outcomes.values()])))
    print(40 * '-')