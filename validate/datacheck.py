'''

API for sanitizing graph data

Author: Justin Chen
Date: 5/9/2020
'''

import numpy as np
from pandas import isnull
from itertools import zip_longest

class DataCheck(object):
    def __init__(self, whitelist=None):
        self.whitelist = whitelist if whitelist is not None else []


    '''
    Helper function to construct adjacency matrix.

    inputs:
    df    (pd.Dataframe) DataFrame of moves
    adj   (ndarray)      Adjacency matrix of moves
    a     (int)          Move id
    edges (list)         List of prereqs or subseqs corresponding to move id
    '''
    def set_edges(self, df, adj, a, edges):
        log = {'edges': []}
        # iterate over every edge
        # if prereq or subseq is not a string, it's NaN
        if isinstance(edges, str):
            for j in edges.split(', '):
                try:
                    b = int(df.loc[df['name'] == j]['id']) - 1
                    adj[a, b] += 1
                except TypeError:
                    log['edges'].append({'src': a+1, 'tgt': j})


    '''
    Create adjacency matrix from pandas dataframe

    inputs:
    df (DataFrame) Table of moves

    outputs:
    adj (ndarray) Adjacency matrix of moves
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
    Check symmetry of adjacency matrix

    inputs:
    m (ndarray) Adjacency matrix of moves

    outputs:
    return (list, ndarray) Returns empty list if symmetric and coordinates of asymmetric values
    '''
    def check_symmetry(self, df):
        m = self.get_adjacency(df)

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
        return [i+1 for i in df[df[col].isnull()].index.tolist() \
                    if not i+1 in self.whitelist]


    '''
    Find all empty cells

    inputs:
    df      (pd.DataFrame) Data source
    columns (list)         List of column headers

    outputs:
    empty (list) List of dicts with key as column header and value as the empty row
    '''
    def find_all_empty(df, columns):
        return [{col: self.find_empty(df, col)} for col in columns]



    '''
    Log rows with duplicate edges

    inputs:
    df (pd.DataFrame) Table of moves

    outputs:
    log (dict) Dictionary of lists of dicts
    '''
    def find_duplicate_edges(self, df):
        log = {'pre': [], 'sub': []}

        for i, row in df.iterrows():
            if isinstance(row['prereq'], str):
                pre = row['prereq'].split(', ')
                uniq = list(set(pre))

                if uniq == pre:
                    e = [i for i in pre if i not in uniq]
                    log['pre'].append({i: e})

            if isinstance(row['subseq'], str):
                sub = row['subseq'].split(', ')
                uniq = list(set(sub))

                if uniq == sub:
                    if uniq == sub:
                        e = [i for i in sub if i not in uniq]
                        log['pre'].append({i: e})

        return log


    '''
    Sort edges alphabetically and update by reference

    inputs:
    df (pd.DataFrame) Table of moves

    outputs:
    df (pd.DataFrame) Sorted table of moves
    '''
    def sort_edges(self, df):
        for i, row in df.iterrows():
            if isinstance(row['prereq'], str):
                edge = row['prereq'].split(', ')
                edge.sort()
                row['prereq'] = edge
                df.at[i, 'prereq'] = ', '.join(row['prereq'])
            
            if isinstance(row['subseq'], str):
                edge = row['subseq'].split(', ')
                edge.sort()
                row['subseq'] = edge
                df.at[i, 'subseq'] = ', '.join(row['subseq'])

        return df


    '''
    Remove unnamed column from DataFrame

    inputs:
    df (pd.DataFrame) DataFrame to clean

    outputs:
    df (pd.DataFrame) Cleaned DataFrame
    '''
    def remove_unnamed(df):
        return df.loc[:, ~df.columns.str.contains('^Unnamed')]


    '''
    Check for plural types and misc white spaces
    e.g. Parallel Bars versus Parallel Bar

    inputs:
    types (str) String of move types separated by "/"

    outputs:
    clean (str) Clean and sorted move type
    '''
    def clean_label(self, types):
        clean = [t[:-1].rstrip() if t.endswith('s') else t.rstrip() for t in types.split('/') if len(t) > 0]
        clean.sort()
        return '/'.join(clean)


    '''
    Get and clean all unique labels

    inputs:
    labels (list) List of labels from DataFrame

    outputs:
    unique (list) List of cleaned unique labels
    errors (list) List of labels with errors
    '''
    def unique_labels(self, labels):
        # build and clean unique map
        labels = list(set(labels))
        errors = set()
        unique = set()
        
        for i, types in enumerate(labels):
            if isinstance(types, float) or len(types) == 0 or types == None:
                continue
                
            try:
                labels[i] = self.clean_label(types)
                
                # check for permutations, output for manual repair
                if labels[i] not in unique:
                    unique.add(labels[i])
                else:
                    errors.append(labels)
            except Exception:
                # check for empty, None, or NaN
                errors.add(types)
        
        unique = list(unique)
        unique.sort()
        return unique, errors


    '''
    Check that label set does not contain permutations and fix so that they point to a canonical label.
    e.g. Wall/Flip == Flip/Wall
    e.g. Wall/Flip/Twist == Wall/Twist/Flip == Flip/Wall/Twist, etc.

    inputs:
    df (pd.DataFrame) DataFrame of moves

    outputs:
    output (list) Empty list if no errors else a list of errors
    '''
    def check_type(self, df):
        labels = df['type'].tolist()
        unique, errors = self.unique_labels(labels)
        
        label_map = {k: set() for k in unique}
        
        for l in labels:
            
            if not isinstance(l, str): 
                continue
                
            k = self.clean_label(l)

            if k in label_map:
                label_map[k].add(l)


        # check if any errors, return empty list if none
        errs = [s for s in label_map.values() if len(s) > 1]

        return [] if len(unique) == sum([len(s) for s in errs]) else errs