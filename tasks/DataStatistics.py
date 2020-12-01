'''
'''
import pandas as pd
import networkx as nx
from numpy import linalg as la
from stats import label_dist
from utils import write

from preproc import relational as rel

class DataStats(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		ml = label_dist(moves, single=False)
		sl = label_dist(moves, single=True)
		
		write('multi_type_dist.json', ml)
		write('single_type_dist.json', sl)

		print(f'multi-label: {len(ml)}\tsingle-label: {len(sl)}')

		G = rel.dataframe_to_graph(moves)
		adj = nx.to_numpy_matrix(G)
		vals = la.eigvals(adj)
		print(sorted(vals.tolist())[:10])
