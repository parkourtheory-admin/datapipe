'''
API for creating and manipulating networkx Graphs from pandas DataFrames

Author: Justin Chen
Date: 5/24/2020
'''
import os
import math
import numbers
import numpy as np
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import pylab
from collections import Counter, defaultdict
from itertools import chain, combinations


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
df 			 (pd.DataFrame)   Moves table
return_edges (bool, optional) True to return edges

outputs:
edges     (int)  Total number of edges in prereq and subseq columns
err       (list) Collection of duplicate moves
ret_edges (list) List of tuples of edges if return_edges parameter is True
'''
def count_edges(df, return_edges=False):

	edges = 0
	err = []
	ret_edges = []

	for i, row in df.iterrows():
		if isinstance(row['prereq'], str):
			p = row['prereq'].split(', ')
			unique_p = len(set(p))

			if return_edges: ret_edges.extend((i, row['name']) for i in p)

			if len(p) != unique_p:
				counts = dict(Counter(list(row['prereq'].split(', '))))
				err.append(f"\n{row['id']} {row['name']} prereq\n{counts}")

			edges += unique_p

		if isinstance(row['subseq'], str):
			s = row['subseq'].split(', ')
			unique_s = len(set(s))

			if return_edges: ret_edges.extend((row['name'], i) for i in s)

			if len(s) != unique_s:
				counts = dict(Counter(list(row['subseq'].split(', '))))
				err.append(f"\n{row['id']} {row['name']} subseq\n{counts}")

			edges += unique_s

	if return_edges: return edges, err, ret_edges
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
	assert all([G.has_edge(*i) for i in dataframe_to_edges(df)])
	assert all([G.has_edge(*i) for i in count_edges(df, True)[-1]])


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


'''
Format column name into relation

inputs:
column (str)

outputs:
relation (str)
'''
def column_to_relation(col):

	return f"has_{col.lower().replace(' ', '_')}"


'''
Add list of types to edges

inputs:

outputs:

'''
def set_relation(G, edge, relation=''):

	relation = column_to_relation(relation)
	key = 'types'

	if not key in G.edges[edge]:
		G.edges[edge][key] = [relation]
	else:
		G.edges[edge][key].append(relation)


'''
Create single data src root node

inputs:

outputs:

'''
def create_src_node(G, source):

	G.add_node(source)


'''
Create a node for each row in the graph

inputs:

outputs:

'''
def create_row_nodes(G, df, source):

	for i in range(len(df)):
		G.add_edge(source, f'row_{i}')


'''
TODO:
This needs fixing. Does not detect nan. Should not create nan nodes!

inputs:

outputs:

'''
def exists(val):

	if isinstance(val, numbers.Number):
		return not math.isnan(val)
	if isinstance(val, str):
		return len(val) > 0 and val != 'nan'
	return val


'''
Create node for each unique value in each column and connect to column node

inputs:

outputs:

'''
def column_to_nodes(G, df, column):

	for val in set(df[column].to_list()):
		if exists(val):
			e = (column, val)
			G.add_edge(*e)
			set_relation(G, e, relation=column)


'''
Create nodes from columns

inputs:

outputs:

'''
def create_col_nodes(G, df):

	attributes = {}

	for i, col in enumerate(df.columns):

		column_to_node(G, df, col)
		attributes[col] = {'column': i, 'dtype': df[col].dtypes}

	nx.set_node_attributes(G, attributes)


'''
For each row, create an edge from the row node to each of its column value nodes

inputs:

outputs:

'''
def connect_row_to_col(G, df):

	for i, row in df.iterrows():
		for col, val in zip(df.columns, row):
			e = (f'row_{i}', val)
			G.add_edge(*e)
			set_relation(G, e, col)


'''
Check if an entire path exists

inputs:

outputs:

'''
def accepts(G, path):

	return all(map(G.has_edge, path, path[1:]))


'''
TODO:
1. Repeats intermediate values e.g [row, cell, cell, cell, ..., col]

Breadth-first find paths from source to target that strictly have relation using standard breadth-first search

inputs:
G        (nx.Graph)
source   (str)
target   (str)
relation (str)

outputs:
path (list) List of nodes as strings or None if no path exists
'''
def bfs(G, source, target, relation):

	queue = [source]
	path = []
	visited = set()

	while queue:
		node = queue.pop(0)

		if node not in visited:
			visited.add(node)

		for e in G.edges(node):
			attrs = G.get_edge_data(*e)

			if attrs and relation in attrs['types']:
				if e[1] not in visited:
					path.append(node)

				if e[1] == target:
					path.append(e[1])
					break

				if e[1] not in visited:
					queue.append(e[1])

	return path


'''
Depth-first finds paths from source to target that strictly have relation using standard depth-first search
Solution based on: https://stackoverflow.com/a/39376201/3158028 by XueYu

inputs:
G        (nx.Graph)
source   (str)
target   (str)
relation (str)

outputs:
path (list) List of nodes as strings or None if no path exists
'''
def dfs(G, source, target, relation):

	stack = [(source, [source])]
	visited = set()

	while stack:
		node, path = stack.pop()

		if node not in visited:

			if node == target: return path
			visited.add(node)

			for e in G.edges(node):
				attrs = G.get_edge_data(*e)

				if attrs and relation in attrs['types']:
					if e[1] not in visited:
						stack.append((e[1], path+[e[1]]))


'''
Recover original table from graph

inputs:
G (nx.Graph) Networkx graph

outputs:
df (pd.DataFrame) DataFrame originally used to generated given graph
'''
def graph_to_table(G):

	# ensure that edges are symmetric
	G = G.to_undirected()

	column_nodes = []
	row_nodes = []

	for n in G.nodes():
		if isinstance(n, str) and 'column' in G.nodes[n]:
			column_nodes.append(n)
		if isinstance(n, str) and n.startswith('row_'):
			row_nodes.append(n)

	data = defaultdict(list)

	for i, row in enumerate(row_nodes):
		for  col in column_nodes:
			path = dfs(G, source=row, target=col, relation=column_to_relation(col))

			# always 2-hop from row node to column node i.e. [row, cell, col]
			data[i].append(np.nan if not path else path[1])

	recovered = pd.DataFrame.from_dict(data, orient='index', columns=column_nodes)

	def set_col_types(df):
		for col in column_nodes:
			df[col] = df[col].astype(G.nodes[col]['dtype'])

	set_col_types(recovered)

	return recovered



'''
Get files in directory

inputs:
directory    (str)  Directory containing files
sort_by_size (bool) If True, sort the files in the directory by their size
last_index   (int)  Only return the files up until this index

outputs:
files (list) List of names
'''
def get_files(directory, sort_by_size=True, last_index=None):

	files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.xlsx')]
	sizes = [(path.split('/')[-1], os.stat(path).st_size) for path in files]
	sizes = sorted(sizes, key=lambda x:x[1])
	files = list(zip(*sizes))[0]
	return files[:last_index]