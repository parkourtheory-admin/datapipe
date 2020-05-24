import networkx as nx
from itertools import chain


'''
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
edge_map (dict) 		 Map for edge values contained in columns
						 Create from dataframe_to_dict()
delim    (str, optional) Delimiter for edge values
'''
def dataframe_to_edges(df, key, cols, edge_map, delim=''):
	for i, row in df.iterrows():
		src = edge_map[row[key]]
		for j in cols:
		    if isinstance(row[j], str):
		        for i in row[j].split(delim):
		        	yield (src, edge_map[i])


edge_map =  dataframe_to_dict(df, 'name', 'id')
df_edges = dataframe_to_edges(df, 'name', ['prereq', 'subseq'], edge_map, delim=', ')
G = nx.Graph(df_edges)
