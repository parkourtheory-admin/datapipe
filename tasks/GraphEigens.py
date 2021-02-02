'''
Eigenvalue analysis of Graph
'''

import pandas as pd
import networkx as nx
from numpy import linalg as la

import matplotlib.pyplot as plt
from preproc import relational as rel

class GraphEigens(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')

		G = rel.dataframe_to_graph(moves)
		adj = nx.to_numpy_matrix(G)
		vals = la.eigvals(adj)

