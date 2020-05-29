import pandas as pd
import numpy as np
import networkx as nx
from collections import defaultdict

def label_dist(df):
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


def main():
	df = pd.read_csv('/media/ch3njus/Seagate4TB/research/parkourtheory/data/database/latest/moves.csv')
	dist = label_dist(df)
	multiclass = [(k, v) for k, v in sorted(dist.items(), key=lambda item: item[1], reverse=True)]
	print(pformat(multiclass))


if __name__ == '__main__':
	main()