"""
Average closing price of stock around a particular date or between 2 dates
"""
''' --------------------------------------------------------------------------------------- '''

from fin_apps.env import *
import sys
from datetime import datetime, timedelta
import numpy as np
import fin_data.nse_pv.nse_spot as nse_spot

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-sy", help="nse symbol")
    arg_parser.add_argument('-ds', help="start date (YYYY-MM-DD)")
    arg_parser.add_argument('-de', help="end   date (YYYY-MM-DD)")
    arg_parser.add_argument('-mp', help="mid-point date (YYYY-MM-DD)")
    arg_parser.add_argument('-ba', type=int, default=5, help="band width in days, default 5")
    arg_parser.add_argument('-se', default='EQ', help="series, default EQ")
    arg_parser.add_argument('-v', action='store_true', help="Verbose / Details of Data")
    arg_parser.add_argument('-t', action='store_true', help="Test")
    args = arg_parser.parse_args()

    nse_spot_obj = nse_spot.NseSpotPVData(verbose=args.v)

    if args.t:
        res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2021-12-31')
        assert abs(res[2] - 3425.62) < 0.1, 'ERROR! %s Unexpected average value' % res
        res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2022-12-31')
        assert abs(res[2] - 3057.05) < 0.1, 'ERROR! %s Unexpected average value' % res
        res = nse_spot_obj.get_avg_closing_price('ASIANPAINT', '2023-03-31')
        assert abs(res[2] - 2784.44) < 0.1, 'ERROR! %s Unexpected average value' % res
        print('All tests passed')
        exit(0)

    if args.sy is None or ((args.ds is None or args.de is None) and args.mp is None):
        arg_parser.print_help()
    elif not (args.ds is None or args.de is None):
        if args.v:
            print()
            df = nse_spot_obj.get_pv_data(args.sy, from_to=[args.ds, args.de], series=None)
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            print('\n\n%s: from_to: %s / %s :::' % (args.sy, df['Date'].values[0], df['Date'].values[1]))
            print('Series found::')
            for se in df['Series'].unique():
                x = df.loc[df['Series'] == se]
                print('    %s: %s to %s' % (se, x['Date'].values[0], x['Date'].values[-1]))
            f = os.path.join(LOG_DIR, 'nse_avgprice.py.csv')
            df.to_csv(f, index=False)
            print('==> PV data saved in: %s' % f)



        else:
            df = nse_spot_obj.get_pv_data(args.sy, from_to=[args.ds, args.de], series=args.se)
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            print('%s: from_to: %s / %s :::' % (args.sy, df['Date'].values[0], df['Date'].values[1]))
            print('Close: average = %.2f, High / Low = %.2f / %.2f' %
                  (df['Close'].mean(), df['Close'].max(), df['Close'].min()))
    else:
        try:
            res = nse_spot_obj.get_avg_closing_price(args.sy, mid_point=args.mp,
                                                     band=args.ba, series=args.se)
            print('%s avg. Close %s to %s: %.2f' % (args.sy, res[0], res[1], res[2]))
        except Exception as e:
            print(f'Exception: {e}\n --> Run with -ds / -de / -v to check PV data during period')
