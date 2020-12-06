'''
Visualize entire mode graph
'''

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from matplotlib import pylab
import numpy as np

from preproc import relational as rel

class VisualizeGraph(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		G = rel.dataframe_to_graph(moves)
		degrees = dict(nx.degree(G))
		node_size = [v*100 for v in degrees.values()]

		# only label nodes with degree two standard deviations above average
		# to preserve readability
		st = np.std(np.array(list(degrees.values())))
		avg = sum(degrees.values())/len(degrees.values())
		labels = {k:k for k, v in degrees.items() if v > avg+4*st}

		plt.figure(num=None, figsize=(100, 100), dpi=100)
		plt.axis('off')
		fig = plt.figure(1)
		pos = nx.spiral_layout(G, center=[0,0])
		pos = nx.spring_layout(G, k=0.2, iterations=200)

		plt.figtext(.5, .9, 'Parkour Theory', fontsize=16, ha='center')

		nx.draw_networkx_nodes(G, pos, nodelist=degrees.keys(), node_size=node_size, alpha=0.25)
		nx.draw_networkx_edges(G, pos, alpha=0.1)
		nx.draw_networkx_labels(G, pos, labels, font_size=8)

		plt.savefig('parkour_theory_graph.pdf', bbox_inches="tight")
		plt.show()
		pylab.close()
		del fig

		plt.show()