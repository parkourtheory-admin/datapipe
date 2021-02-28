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
			G = nx.Graph(json.load(file))

			'''
			convert string names to int ids. however, this first line only converts string int ids.
				then convert the string int ids to ints
			'''
			re_G = nx.relabel_nodes(G, node_map)
			re_G = nx.relabel_nodes(re_G,{i:int(i) for i in re_G.nodes()})

			'''
			invert the original dictionary, convert the int id node version back to the original strings
			this is just for performing the assertion to check that the conversion is correct and that
			the original graph is recoverable.
			'''
			undo = nx.relabel_nodes(re_G, {v:k for k,v in node_map.items()})

			assert G.edges() == undo.edges()


		with open(os.path.join(self.cfg.video_csv_out, 'adjlist.json'), 'w') as file:
			json.dump(nx.to_dict_of_lists(re_G), file)
