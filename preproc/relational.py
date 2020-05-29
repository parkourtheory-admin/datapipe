'''
API for creating and manipulating networkx Graphs from pandas DataFrames

Author: Justin Chen
Date: 5/24/2020
'''
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import pylab


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
Generates edges given a pandas DataFrame that contains edges as value-separated strings

inputs:
df       (pd.DataFrame)  DataFrame source
cols     (list) 		 List of columns names as strings
delim    (str, optional) Delimiter for edge values
'''
def dataframe_to_edges(df, key, cols, delim=''):
	for i, row in df.iterrows():
		src = row[key]
		for j in cols:
		    if isinstance(row[j], str):
		        for i in row[j].split(delim):
		        	yield (src, i)


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
Create undirected networkx Graph from pandas DataFrame

inputs:
df (pd.DataFrame) DataFrame of moves

outputs:
G (nx.Graph) Graph based on DataFrame
'''
def dataframe_to_graph(df):
	edges = dataframe_to_edges(df, 'name', ['prereq', 'subseq'], ', ')
	G = nx.Graph(edges)

	roots = no_edge(df, 'prereq')
	singles = no_edge(roots, 'subseq')

	for i, node in singles.iterrows():
    	G.add_node(node['name'])

    return G


'''
credit: https://stackoverflow.com/a/17388676

inputs:
G        (nx.Graph) Graph
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


def main():
 	src='database/latest/moves.csv'
	df = pd.read_csv(src, dtype={'id': int})
	# edge_map =  dataframe_to_dict(df, 'name', 'id')
 	G = dataframe_to_graph(df)
	save_graph(G, 'path.pdf')


if __name__ == '__main__':
	main()