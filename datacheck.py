'''

API for sanitizing graph data

Author: Justin Chen
Date: 5/9/2020
'''

import networkx as nx
import numpy as np
from pandas import isnull
from itertools import zip_longest

class DataCheck(object):
    def __init__(self, log, whitelist=None):
        self.log = log
        self.whitelist = whitelist


    def set_edges(self, df, adj, a, edges):
                # iterate over every edge
        # if prereq or nextmove is not a string, it's NaN
        if isinstance(edges, str):
            for j in edges.split(', '):
                try:
                    b = int(df.loc[df['name'] == j]['id']) - 1
                    adj[a, b] += 1
                except TypeError:
                    self.log.debug(f'src: {a+1}\ttgt: {j}')


    '''
    Create adjacency matrix from pandas dataframe

    inputs:
    df (DataFrame)

    outputs:
    adj (ndarray)
    '''
    def get_adjacency(self, df):
        d = len(df)
        adj = np.zeros([d, d], dtype=int)

        for i, row in df.iterrows():
            a = int(row['id']) - 1

            self.set_edges(df, adj, a, row['prereq'])
            self.set_edges(df, adj, a, row['subseq'])

        return adj


    '''
    Check symmetry of
    '''
    def check_symmetry(self, m):
        if m is None: return
        if not (m == m.T).all():
            return np.argwhere(np.triu(m+m.T) == 1)+1
        return []


    '''
    Find duplicate rows

    inputs:
    col (str)         Column name
    df (pd.DataFrame) Dataframe

    outputs: series
    '''
    def duplicated(self, col, df):
        return df[df.duplicated(col, False)]


    '''
    Check that ids contain entire range of values.
    If it does not, return the ids that do not match the ground truth.

    inputs: df (pd.DataFrame)
    outputs: list of incorrect ids
    '''
    def invalid_ids(self, df):
        ids = df['id'].tolist()
        correct = range(1, len(ids)+1)

        return [(a,b) for a, b in zip_longest(ids, correct, fillvalue='MISSING') if int(a) != b]




    '''
    Find cells with missing values

    inputs:
    df (pd.DataFrame) Dataframe
    col (str)         Column name

    outputs:
    list of row indices with missing values for column
    '''
    def find_empty(self, df, col):
        return [i+1 for i in df[df[col].isnull()].index.tolist()]


    '''
    Issue: https://github.com/parkourtheory-admin/datapipe/issues/23

    '''
    def find_duplicate_edges(self, df):
        pass
