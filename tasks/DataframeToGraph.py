'''
Extract relational features, which can later be use for graph embeddings
'''

import pandas as pd
import networkx as nx

from preproc import relational as rel

class DataframeToGraph(object):
	def __init__(self, config):
		self.cfg = config

	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		
		G = rel.dataframe_to_graph(moves)
		binary = nx.to_numpy_matrix(G, dtype=np.int64)
		df = pd.DataFrame(data=binary)

		df.to_csv('logs/edge_features.csv', index=False)