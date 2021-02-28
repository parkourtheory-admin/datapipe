'''
Given any map, relabel the nodes in a graph.

First iteration will assume given a networkx graph.
'''
import os
import json
import networkx as nx

class RelabelGraph(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		node_map = None
		with open(os.path.join(self.cfg.video_csv_out, self.cfg.node_map), 'r') as file:
			node_map = json.load(file)

		re_G = None
		with open(os.path.join(self.cfg.video_csv_out, 'adjlist.json'), 'r') as file:
			G = nx.Graph(json.load(file)).to_undirected()
			re_G = nx.relabel_nodes(G, node_map)

		with open(os.path.join(self.cfg.video_csv_out, 'adjlist.json'), 'w') as file:
			json.dump(nx.to_dict_of_lists(re_G), file)
