'''
Aggregated and multi-hot label distributions
https://github.com/parkourtheory-admin/datapipe/issues/100

TODO:
1. Bar plot for each graph component for aggregated labels
2. Bar plot for each graph component for multi-hot labels
'''
import os
from collections import defaultdict

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pprint import pprint

from preproc import relational as rel


class LabelDistributionPerComponent(object):
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

		for i, comp in aggregated.items():
			self.plot(comp.keys(), comp.values(), 'labels', 'frequency', f'Aggregated Labels for Component {i}')
		
		for i, comp in multihot.items():
			self.plot(comp.keys(), comp.values(), 'labels', 'frequency', f'Aggregated Labels for Component {i}')


	def plot(self, x, y, xlabel, ylabel, title, labelsize=10):
		plt.figure(figsize=(20,10))
		plt.bar(x, y)
		plt.tick_params(axis='x', which='major', labelsize=labelsize)
		plt.xticks(rotation=45, ha='right')
		plt.xlabel('labels')
		plt.ylabel(ylabel)
		plt.title(title)
		plt.subplots_adjust(left=0.1, bottom=0.3)
		plt.savefig(os.path.join(self.cfg.output_dir, f'{title}.pdf'))
		plt.show()
