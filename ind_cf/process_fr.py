"""
To Do / WIP / etc
"""
''' --------------------------------------------------------------------------------------- '''

from fin_data.env import *
import fin_data.ind_cf.base_utils as base_utils

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    from argparse import ArgumentParser
    arg_parser = ArgumentParser()
    arg_parser.add_argument("-sy", help="nse symbol")
    arg_parser.add_argument('-pe', help="period end (YYYY-MM-DD)")
    arg_parser.add_argument('-u', help="XBRL url")
    arg_parser.add_argument('-t', default='OneI', help="Filter for tags")
    arg_parser.add_argument('-v', action='store_true', help="Verbose")
    args = arg_parser.parse_args()

    if args.u is not None:
        xbrl_data = base_utils.get_xbrl(args.u)
    else:
        xbrl_data = []
    assert len(xbrl_data) > 0, 'ERROR, empty xbrl_data'

    parsed_result = base_utils.parse_xbrl_fr(xbrl_data)
    [print('%s: %s' % (k, parsed_result[k])) for k in parsed_result.keys() if k != 'parsed_df']
    df = parsed_result['parsed_df']
    df.to_csv(os.path.join(LOG_DIR, 'parsed_df_%s.csv' % parsed_result['NSE Symbol']), index=False)
    print(df.loc[df['context'] == args.t].to_string(index=False))