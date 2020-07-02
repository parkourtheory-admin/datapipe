'''
'''
import pandas as pd
from validate import datacheck as dck
from utils import write

class DuplicateEdges(object):

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

        edges = dc.find_duplicate_edges(df)
        log['duplicate_edges'] = edges

        write('duplicate_edges.json', log)