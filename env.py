"""
Global Settings - Key variables
"""
''' --------------------------------------------------------------------------------------- '''

import os
import sys

DEV = False

if DEV:
    print()
    print('####################################################################################')
    print('############### FIN_DATA ::: DEV Environment Active ################################')
    print('####################################################################################')
    print()

DATA_ROOT   = os.path.join(os.getenv('HOME_DIR'), r'20_env\data_dev') if DEV else os.getenv('DATA_ROOT')
LOG_DIR     = os.path.join(os.getenv('LOG_ROOT'), '01_fin_data')
CONFIG_ROOT = os.getenv('PROJECTS_CONFIG')

def who_am_i():
    return os.path.dirname(__file__)

''' --------------------------------------------------------------------------------------- '''
if __name__ == '__main__':
    print()
    print('DATA_ROOT:   ', DATA_ROOT)
    print('LOG_DIR:     ', LOG_DIR)
    print('CONFIG_ROOT: ', CONFIG_ROOT)

    print('who_am_i:    ', who_am_i())

    print('\nPYTHONPATH :', os.getenv('PYTHONPATH'))
    print('\nsys.path :::\n%s' % '\n'.join(sys.path))

    """
    TO DO: verify who_am_i is who_i_am_supposed_to_be
    """
