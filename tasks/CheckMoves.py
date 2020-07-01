'''
'''
import pandas as pd
from validate import datacheck as dck
from utils import write

class CheckMoves(object):

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
        df = pd.read_csv(src, header=0)

        dc = dck.DataCheck(whitelist=self.cfg.whitelist)

        ids = dc.invalid_ids(df)
        log['invalid_ids'] = ids

        edges = dc.find_duplicate_edges(df)
        log['duplicate_edges'] = edges

        dup = dc.duplicated('name', df)
        log['duplicate_nodes'] = dup.to_json()

        err = dc.check_symmetry(df)
        log['symmetry'] = err

        cols = ['id', 'name', 'type', 'desc']
        log['incomplete'] = dc.fine_all_empty(df, cols)

        df = dc.sort_edges(df)
        df = dc.remove_unnamed(df)
        df.to_csv(src, index=False)

        errs = dc.check_type(df)
        log['move_types'] = errs

        write('data_check.json', log)