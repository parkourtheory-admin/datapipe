import pandas as pd
import numpy as np
import networkx as nx
from collections import defaultdict

'''
Count number of labels

inputs:
df 	   (pd.DataFrame)   DataFrame of moves
single (bool, optional) If True, count individual types e.g. Wall, Vault, etc.
						If False, count combination of types e.g. Wall/Flip, etc.

outputs:
dist (dict) Dictionary of counts of labels
'''
def label_dist(df, single=True):
    dist = defaultdict(int)
    
    for i, row in df.iterrows():
        if isinstance(row['type'], str):
            if single:
                types = row['type'].split('/')
                for t in types:
                    dist[t] += 1
            else:
                dist[row['type']] += 1
    return dist


'''
Get average node degree in graph

inputs:
G 

outputs:
avg (float) Average node degree
'''
def avg_node_degree(G):
    return sum([G.degree[i] for i in G.nodes()])/len(G.nodes())


'''
'''
def max_node_degree(G):
    d = dict(G.degree)
    k = max(d, key=d.get)
    return k, d[k]


'''
inputs:
df (pd.DataFrame)

outputs:
avgs (defaultdict)
'''
def avg_degree_type(df):
    avgs = defaultdict(float)
    for group in df.groupby('type'):
        pre = 0
        sub = 0
        total = 0
        count = 0

        for i, row in group[1].iterrows():
            count += 1
            if isinstance(row['prereq'], str):
                pre = len(row['prereq'].split(', '))
            if isinstance(row['subseq'], str):
                sub = len(row['subseq'].split(', '))
            total += (pre + sub)

        total /= count
        avgs[group[0]] = total

    return avgs