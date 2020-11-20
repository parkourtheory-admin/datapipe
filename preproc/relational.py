'''
API for creating and manipulating networkx Graphs from pandas DataFrames

Author: Justin Chen
Date: 5/24/2020
'''
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import pylab
from collections import Counter
from itertools import chain


'''
Creates map for nodes and corresponding labels from a pandas DataFrame

inputs:
df  (pd.DataFrame) DataFrame
key (str) 		   Column name of column values to be used as dict keys
val (str)		   Column name of column values to be used as dict vals

outputs:
map (dict) Dictionary mapping column to column
'''
def dataframe_to_dict(df, key, val):
	return dict(df[[key, val]].values.tolist())


'''
Generator for edges given a pandas DataFrame that contains edges as value-separated strings

inputs:
df	   (pd.DataFrame)  DataFrame source
delim  (str, optional) Delimiter for edge values
'''
def dataframe_to_edges(df, delim=', '):

	for i, row in df.iterrows():
		src, pre, sub = row.iloc[1].strip(), row.iloc[2], row.iloc[3]

		if isinstance(pre, str):
			for p in pre.split(delim):
				yield (p.strip(), src)
		
		if isinstance(sub, str):
			for s in sub.split(delim):
				yield (src, s.strip())


'''
inputs:
df (pd.DataFrame) Moves table

outputs:
edges (int)  Total number of edges in prereq and subseq columns
err   (list) Collection of duplicate moves
'''
def count_edges(df):
	edges = 0
	err = []

	for i, row in df.iterrows():
		if isinstance(row['prereq'], str):
			p = row['prereq'].split(', ')
			unique_p = len(set(p))

			if len(p) != unique_p:
				counts = dict(Counter(list(row['prereq'].split(', '))))
				err.append(f"\n{row['id']} {row['name']} prereq\n{counts}")

			edges += unique_p

		if isinstance(row['subseq'], str):
			s = row['subseq'].split(', ')
			unique_s = len(set(s))

			if len(s) != unique_s:
				counts = dict(Counter(list(row['subseq'].split(', '))))
				err.append(f"\n{row['id']} {row['name']} subseq\n{counts}")

			edges += unique_s

	return edges, err


'''
Find all moves without prereq moves

inputs:
df (pd.DataFrame) DataFrame of moves

outputs:
df (pd.DataFrame) DataFrame containing only rows without prereq nodes
'''
def no_edge(df, edge_type):
	return df.loc[df[edge_type].isnull()]


'''
Check that graph was correctly created from the dataframe by iterating over every move. Check if the move is in the graph.
Then get the edges and compare to the edges in the dataframe.

inputs:
G  (nx.Graph)     Networkx graph of given dataframe
df (pd.DataFrame) Dataframe of entities and relations
'''
def validate_graph(G, df):
	moves = list(df['name'])

	for m in moves:
		assert G.has_node(m)

		row = df[df['name'] == m]
		pre = row['prereq'].squeeze()
		sub = row['subseq'].squeeze()
		pre = pre.split(', ') if isinstance(pre, str) else []
		sub = sub.split(', ') if isinstance(sub, str) else []
		df_edges = set([i for i in chain.from_iterable([pre, sub]) if len(i) > 0])
		graph_edges = set(G.neighbors(m))

		assert graph_edges == df_edges

	assert len(G.nodes()) == len(df)


'''
Create undirected networkx Graph from pandas DataFrame

inputs:
df 		 (pd.DataFrame) DataFrame of moves
directed (bool) 		True if directed edges, else undirected

outputs:
G (nx.Graph) Graph based on DataFrame
'''
def dataframe_to_graph(df, directed=False, validate=False):
	edges = dataframe_to_edges(df, delim=', ')
	G = nx.DiGraph(edges) if directed else nx.Graph(edges)

	singles = no_edge(no_edge(df, 'prereq'), 'subseq')

	for i, node in singles.iterrows():
		G.add_node(node['name'])

	if validate: validate_graph(G, df)

	return G


'''
credit: https://stackoverflow.com/a/17388676

inputs:
G		(nx.Graph) Graph
filename (str) 		File name to save graph drawing
'''
def save_graph(G, filename):
	plt.figure(num=None, figsize=(100, 100), dpi=100)
	plt.axis('off')
	fig = plt.figure(1)
	
	pos = nx.spiral_layout(G, center=[0,0])
	pos = nx.spring_layout(G, k=2, iterations=0)
	
	plt.figtext(.5, .85, 'PARKOUR THEORY', fontsize=130, fontweight='bold', ha='center')
	
	nx.draw_networkx_nodes(G, pos, alpha=0.25)
	nx.draw_networkx_edges(G, pos, alpha=0.1)
	nx.draw_networkx_labels(G, pos, font_color='r', font_size=11)

	plt.savefig(filename, bbox_inches="tight")
	pylab.close()
	del fig