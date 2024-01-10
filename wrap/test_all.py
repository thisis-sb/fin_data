"""
test all: nse_symbols, nse_spot
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import os
import random
from datetime import datetime, timedelta
import fin_data.nse_pv.nse_spot as nse_spot
from fin_data.common import nse_config, nse_symbols, nse_cf_ca
from fin_data.nse_pv import get_hpv, get_dr, process_dr, nse_spot
from pygeneric.datetime_utils import elapsed_time, remove_timers

''' --------------------------------------------------------------------------------------- '''
def test_nse_spot(verbose=False):
    elapsed_time('test_nse_spot_0')
    import fin_data.nse_pv.get_hpv as get_hpv

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting NseSpotPVData basics ...')
    symbols = ['ASIANPAINT', 'BRITANNIA', 'HDFC', 'HDFCBANK', 'ICICIBANK', 'IRCON', 'IRCTC',
               'JUBLFOOD', 'TATASTEEL', 'ZYDUSLIFE']
    nse_spot_obj = nse_spot.NseSpotPVData(verbose=False)

    def check_data(symbol_list, dates):
        print('Checking for dates', dates, '...', end=' ')
        for symbol in symbol_list:
            if verbose:
                print(f'\n  {symbol} ...', end=' ')

            df1 = nse_spot_obj.get_pv_data(symbol, series='EQ', from_to=dates)
            df2 = get_hpv.get_pv_data(symbol, from_to=dates)
            df1.to_csv(os.path.join(LOG_DIR, 'nse_spot_df1.csv'), index=False)
            df2.to_csv(os.path.join(LOG_DIR, 'nse_spot_df2.csv'), index=False)
            assert abs(df1.shape[0] - df2.shape[0]) <= 5, "%d / %d" % (df1.shape[0], df2.shape[0])

            if df1.shape[0] != df2.shape[0]:
                common_dates = list(set(df1['Date']) & set(df2['Date']))
                df1 = df1.loc[df1['Date'].isin(common_dates)].reset_index(drop=True)
                df2 = df2.loc[df2['Date'].isin(common_dates)].reset_index(drop=True)
            for c in ['Open', 'High', 'Low', 'Close', 'Prev Close',
                      'Volume', 'Delivery Volume', 'No Of Trades']:
                assert (~(abs(df1[c] - df2[c]) < 0.5)).sum() == 0, \
                    '%s: Column: %s: %d' % (symbol, c, (~(abs(df1[c] - df2[c]) < 0.5)).sum())

            for c in ['Traded Value']:
                assert (~(abs(df1[c] - df2[c]) < 100)).sum() <= 1, \
                    '%s: Column: %s: %d' % (symbol, c, (~(abs(df1[c] - df2[c]) < 100)).sum())

            if verbose:
                print('OK', end='')
        print('') if verbose else print('OK')
        return

    end_date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
    check_data(symbols, ['2018-01-01', '2018-12-31'])
    check_data(symbols, ['2019-01-01', '2019-12-31'])
    check_data(symbols, ['2020-01-01', '2020-12-31'])
    check_data(symbols, ['2021-01-01', '2021-12-31'])
    check_data(symbols, ['2022-01-01', '2022-12-31'])
    check_data(symbols, ['2023-01-01', '2023-12-31'])
    check_data(symbols, ['2024-01-01', end_date])

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting NseSpotPVData().get_pv_data (for multiple symbols) ...', end=' ')
    """ To do: Verify 52_Wk_H/L"""
    multi_df = nse_spot_obj.get_pv_data(symbols, from_to=['2018-01-01', end_date])
    for symbol in symbols:
        if verbose:
            print(f'\n  {symbol} ...', end=' ')
        df1 = multi_df.loc[multi_df['Symbol'] == symbol].reset_index(drop=True)
        df2 = get_hpv.get_pv_data(symbol, from_to=['2018-01-01', end_date])

        assert abs(df1.shape[0] - df2.shape[0]) <= 5, "%d / %d" % (df1.shape[0], df2.shape[0])

        if df1.shape[0] != df2.shape[0]:
            common_dates = list(set(df1['Date']) & set(df2['Date']))
            df1 = df1.loc[df1['Date'].isin(common_dates)].reset_index(drop=True)
            df2 = df2.loc[df2['Date'].isin(common_dates)].reset_index(drop=True)

        for c in ['Open', 'High', 'Low', 'Close', 'Prev Close',
                  'Volume', 'Delivery Volume', 'No Of Trades']:
            assert (~(abs(df1[c] - df2[c]) < 0.5)).sum() == 0, \
                'Column: %s: %d' % (c, (~(abs(df1[c] - df2[c]) < 0.5)).sum())

        for c in ['Traded Value']:
            assert (~(abs(df1[c] - df2[c]) < 100)).sum() <= 1, \
                'Column: %s: %d' % (c, (~(abs(df1[c] - df2[c]) < 100)).sum())

        if verbose:
            print('OK', end='')
    print('') if verbose else print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting get_index_pv_data ... ', end='')
    pv_data = nse_spot_obj.get_index_pv_data('NIFTY 50', ['2023-04-01', '2023-05-02'])
    assert pv_data.shape[0] == 18 and pv_data.shape[1] == 15
    pv_data = nse_spot_obj.get_index_pv_data('NIFTY 50', ['2023-04-01', None])
    assert pv_data.shape[0] > 0 and pv_data.shape[1] == 15
    pv_data = nse_spot_obj.get_index_pv_data(['NIFTY 50', 'NIFTY MIDCAP 150', 'NIFTY IT'],
                                             ['2023-04-01', '2023-05-02'])
    assert pv_data.shape[0] == 54 and pv_data.shape[1] == 15
    pv_data = nse_spot_obj.get_index_pv_data('NIFTY 50', ['2010-01-01', '2019-12-31'])
    assert pv_data.shape[0] == 2443 and pv_data.shape[1] == 15
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting get_etf_pv_data ... ', end='')
    pv_data = nse_spot_obj.get_etf_pv_data('NIFTYBEES', ['2023-04-01', '2023-05-02'])
    assert pv_data.shape[0] == 18 and pv_data.shape[1] == 17, pv_data.shape
    pv_data = nse_spot_obj.get_etf_pv_data('NIFTYBEES', ['2023-04-01', None])
    assert pv_data.shape[0] > 0 and pv_data.shape[1] == 17, pv_data.shape
    pv_data = nse_spot_obj.get_etf_pv_data(['NIFTYBEES', 'ITBEES', 'CPSEETF'],
                                           ['2023-04-01', '2023-05-02'])
    assert pv_data.shape[0] == 54 and pv_data.shape[1] == 17, pv_data.shape
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting get_spot_quote ... ', end='')
    keys  = ['Symbol', 'Series', 'Date', 'epoch', 'Open', 'High', 'Low', 'Close',
             'previousClose', 'lastPrice', 'pChange']
    assert list(nse_spot.get_spot_quote('ASIANPAINT').keys()) == keys
    assert list(nse_spot.get_spot_quote('NIFTY 50', index=True).keys()) == keys
    res = nse_spot.get_spot_quote(['NIFTY IT', 'NIFTY MIDCAP 150', 'NIFTY AUTO'], index=True)
    assert type(res) == list and len(res) == 3 and list(res[2].keys()) == keys
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting for partly paid symbol ...', end=' ')
    df_pp = nse_spot_obj.get_pv_data('AIRTELPP', series='E1', from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    df_pp = nse_spot_obj.get_pv_data(['BHARTIARTL', 'AIRTELPP'], from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    df_pp = nse_spot_obj.get_pv_data(['BRITANNIA'], from_to=['2022-10-01', '2022-10-10'])
    assert df_pp.shape[0] == 5, 'partly paid Not OK'
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting for NseSpotPVData().get_pv_data (for large # of multiple symbols) ...')
    elapsed_time(0)
    import fin_data.common.nse_symbols as nse_symbols
    symbols = nse_symbols.get_symbols(['NIFTY 50', 'NIFTY NEXT 50'])
    df = nse_spot_obj.get_pv_data(symbols, from_to=['2018-01-01', None], verbose=True)
    print('Done. time check:', elapsed_time(0), 'seconds\n')

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting for NseSpotPVData().get_pv_data (for same symbol, different series) ...')
    ''' TO DO: Need a better solution (for current ones & not discontinued)'''
    assert nse_spot_obj.get_pv_data('HDFC', series='EQ',
                                    from_to=['2023-05-12', None])['Close'].values[0] == 2776.3
    assert nse_spot_obj.get_pv_data('HDFC', series='W3',
                                    from_to=['2023-05-12', None])['Close'].values[0] == 560.55
    assert nse_spot_obj.get_pv_data('BRITANNIA', series='EQ',
                                    from_to=['2023-05-12', None])['Close'].values[0] == 4616.50
    assert nse_spot_obj.get_pv_data('BRITANNIA', series='N3',
                                    from_to=['2023-05-12', None])['Close'].values[0] == 29.54
    print('OK')

    ''' ----------------------------------------------------------------------------------- '''
    print('\nTesting for NseSpotPVData().get_avg_closing_price ...')
    res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2021-12-31')
    assert abs(res[2] - 3425.62) < 0.1, 'ERROR! %s Unexpected average value' % res

    res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2022-12-31')
    assert abs(res[2] - 3057.05) < 0.1, 'ERROR! %s Unexpected average value' % res

    res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2023-03-31')
    assert abs(res[2] - 2784.44) < 0.1, 'ERROR! %s Unexpected average value' % res
    print('OK')

    print('test_nse_spot: total time taken: %.2f' % elapsed_time('test_nse_spot_0'))
    remove_timers('test_nse_spot_0')

    return True

def test_perf_nse_pv(verbose=False):
    print('\nStarting test_perf_nse_pv :::')
    print('Testing NseSpotPVData() ...')
    elapsed_time(['tpnp_0', 'tpnp_1'])
    nse_spot_obj = nse_spot.NseSpotPVData(verbose=verbose)
    print('Step1: initialization: %.2f' % elapsed_time('tpnp_1'))

    for dt in ['2021-01-01', '2018-01-01']:
        symbols = nse_symbols.get_symbols(['NIFTY 50'])
        print('\nRunning Step2 for date:', dt, '...')
        for i in range(0, 5):
            one_symbol = random.choice(symbols)
            elapsed_time('tpnp_1')
            df = nse_spot_obj.get_pv_data(one_symbol, from_to=[dt, None], verbose=verbose)
            print('  Step2: get_pv_data: [%s] since [%s]: shape: %s, time taken: %.2f'
                  % (one_symbol, dt, df.shape, elapsed_time('tpnp_1')))

        print('Running Step3 for date:', dt, '...')
        n_few_symbols = 0
        for i in range(0,5):
            n_few_symbols += 10
            few_symbols = [random.choice(symbols) for x in range(0, n_few_symbols)]
            elapsed_time('tpnp_1')
            df = nse_spot_obj.get_pv_data(few_symbols, from_to=[dt, None], verbose=verbose)
            print('  Step3: get_pv_data: %d symbols since [%s]: shape: %s, time taken: %.2f'
                  % (len(few_symbols), dt, df.shape, elapsed_time('tpnp_1')))

        elapsed_time('tpnp_1')
        print('Running Step4 for date:', dt, '...')
        for ix in ['NIFTY 50', 'NIFTY 100', 'NIFTY MIDCAP 150',
                   'NIFTY SMALLCAP 250', 'NIFTY TOTAL MARKET']:
            symbols = nse_symbols.get_symbols([ix])
            df = nse_spot_obj.get_pv_data(symbols, from_to=[dt, None], verbose=verbose)
            print('  Step4: get_pv_data: [%s] since [%s]: shape: %s, time taken: %.2f'
                  % (ix, dt, df.shape, elapsed_time('tpnp_1')))

    print('Step5: Testing 100 calls to get_avg_closing_price: ', end=' ')
    symbols = nse_symbols.get_symbols(['NIFTY 500'], series='EQ')
    elapsed_time('tpnp_1')
    for i in range(100):
        s = random.choice(symbols)
        try:
            _ = nse_spot_obj.get_avg_closing_price(s, mid_point='2023-06-30')
        except Exception as e:
            print('\nFor %s, ERROR: %s' % (s, e))
    print('total time: %.2f' % elapsed_time('tpnp_1'))

    print('test_nse_spot: total time taken: %.2f' % elapsed_time('tpnp_0'))
    remove_timers(['tpnp_0', 'tpnp_1'])

    return

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    assert nse_symbols.test_me(), 'nse_symbols.test_me() failed'
    assert nse_cf_ca.test_me(), 'nse_cf_ca.test_me() failed'
    test_nse_spot()
    test_perf_nse_pv()
    print('\n>>>>>>>>>> Test ind_cf: See fin_apps <<<<<<<<<<')