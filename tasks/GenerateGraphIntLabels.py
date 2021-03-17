'''
Given any map, relabel the nodes in a graph.

First iteration will assume given a networkx graph.
'''
import os
import json
import networkx as nx

class GenerateGraphIntLabels(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		node_map = None
		with open(os.path.join(self.cfg.output_tasks_dir, 'Name2Int', self.cfg.node_map), 'r') as file:
			node_map = json.load(file)

		re_G = None
		with open(os.path.join(self.cfg.output_tasks_dir, 'GenerateGraph', 'adjlist.json'), 'r') as file:
			G = nx.Graph(json.load(file))
			re_G = nx.relabel_nodes(G, node_map)

			'''
			invert the original dictionary, convert the int id node version back to the original strings
			this is just for performing the assertion to check that the conversion is correct and that
			the original graph is recoverable.
			'''
			undo = nx.relabel_nodes(re_G, {v:k for k,v in node_map.items()})

			assert G.edges() == undo.edges()


		with open(os.path.join(self.cfg.output_tasks_dir, self.__class__.__name__, 'adjlist.json'), 'w') as file:
			json.dump(nx.to_dict_of_lists(re_G), file)
