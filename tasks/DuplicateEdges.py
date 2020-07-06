'''
'''
import pandas as pd

from utils import write
from validate import datacheck as dck
from preproc import relational as rel

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

        G = rel.dataframe_to_graph(df)
        edges, duplicates = rel.count_edges(df)
        ground_truth = edges//2
        graph_count = len(G.edges())

        print(f'dataframe_to_graph: {graph_count}\ttruth: {ground_truth}')

        assert graph_count == ground_truth

        log['duplicate_edges'] = duplicates
        write('duplicate_edges.json', log)