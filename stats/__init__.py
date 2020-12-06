import pandas as pd
import numpy as np
import networkx as nx
from collections import defaultdict

'''
Count number of labels

inputs:
df 	     (pd.DataFrame)   DataFrame of moves
multihot (bool, optional) If True, use multi-hot encoding e.g. Wall, Vault, etc.
						  If False, use one-hot encoding e.g. Wall/Flip, etc.

outputs:
dist (dict) Dictionary of counts of labels
'''
def label_dist(df, multihot=True):
    dist = defaultdict(int)
    
    for type_ in df['type']:
        type_ = str(type_)

        if multihot:
            types = type_.split('/')
            for t in types:
                dist[t] += 1
        else:
            dist[type_] += 1

    return dist


'''
Compute percentage each label comprises of label set

inputs:
labels (dict) Dictionary of frequencies of labels generated from label_dist()

outputs:
percentages (dict) Dictionary of percentage each label comprises of the label set
'''
def label_percentages(labels):
    total = sum(labels.values())
    return {k: v/total for k, v in labels.items()}


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