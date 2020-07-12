'''
'''
import pandas as pd
from validate import datacheck as dck
from utils import write

class MoveTypes(object):

    '''
    inputs:
    config (Configuration) Object containing parsed configuration values
    '''
    def __init__(self, config):
        self.cfg = config
        

    def run(self):
        log = {}
        
        # check over move table
        src = self.cfg.move_csv
        df = pd.read_csv(src, header=0, sep='\t')

        dc = dck.DataCheck(whitelist=self.cfg.whitelist)

        errs = dc.check_type(df)
        log['move_types'] = errs

        write('move_types.json', log)