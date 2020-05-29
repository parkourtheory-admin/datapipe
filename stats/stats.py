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