'''
Datapipe issue #100 for aggregated and multi-hot labels
'''
from collections import defaultdict

import pandas as pd
import networkx as nx
from preproc import relational as rel

class ComponentNodeLabelStats(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		G = rel.dataframe_to_graph(moves)

		aggregated = defaultdict(dict)
		multihot = defaultdict(dict)

		for i, comp in enumerate(nx.connected_components(G)):
			agg = defaultdict(int)
			hot = defaultdict(int)

			for node in comp:
				type_ = str(moves[moves['name'] == node]['type'].squeeze())
				agg[type_] += 1

				for t in type_.split('/'):
					hot[t] += 1

			aggregated[i] = agg
			multihot[i] = hot


	def plot(self):
		pass