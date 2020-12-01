'''
'''
import os
from collections import OrderedDict

import pandas as pd
import networkx as nx
from numpy import linalg as la
from stats import label_dist
from utils import write

import matplotlib.pyplot as plt
from preproc import relational as rel

class DataStats(object):
	def __init__(self, config):
		self.cfg = config


	def plot(self, x, y, xlabel, ylabel, title, labelsize=10):
		plt.figure(figsize=(20,10))
		plt.bar(x, y)
		plt.tick_params(axis='x', which='major', labelsize=labelsize)
		plt.xticks(rotation=45, ha='right')
		plt.xlabel('labels')
		plt.ylabel(ylabel)
		plt.title(title)
		plt.tight_layout()
		plt.savefig(os.path.join(self.cfg.video_csv_out, f'{title}.pdf'))
		plt.show()


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		ml = label_dist(moves, single=False)
		sl = label_dist(moves, single=True)
		
		# Label distributions
		write('multi_type_dist.json', ml)
		write('single_type_dist.json', sl)
		ml = OrderedDict((k, v) for k, v in sorted(ml.items(), key=lambda x: x[1]))
		sl = OrderedDict((k, v) for k, v in sorted(sl.items(), key=lambda x: x[1]))

		self.plot(ml.keys(), ml.values(), 'labels', 'samples', 'Multi-type Labels', labelsize=8)
		self.plot(sl.keys(), sl.values(), 'labels', 'samples', 'Single-type Labels')

		# Eigenvalues
		G = rel.dataframe_to_graph(moves)
		adj = nx.to_numpy_matrix(G)
		vals = la.eigvals(adj)
