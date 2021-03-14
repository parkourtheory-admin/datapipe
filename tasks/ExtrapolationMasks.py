'''
Generate graph masks for extrapolation task. Here the traininig mask is the largest component, and validation and testing are nodes outside this main component.
This is supposed to simulate new node entering the graph that have no prerequisite moves.

Note:
1. Execute Name2Int, GenerateGraph and RelabelGraph tasks before executing this to get nodes relabeled as consecutive intgers. 
2. Whenever generating masks, must also generate labels which is canonical ordering for masks.
'''
import os
import json
import numpy as np
import networkx as nx

from utils import *

class ExtrapolationMask(object):
	def __init__(self, config):
		self.cfg = config


	def save(self, data, save_path):
		with open(save_path, 'w') as file:
			json.dump(data, file, ensure_ascii=False, indent=4)


	'''
	Masks for training on largest connected component and validation on all other components.

	inputs:
	G           (nx.Graph) Graph data with nodes relabeled as consecutive integers starting from zero
	train_split (float)    Training split percentage
	val_split   (float)    Validation split percentage
	test_split  (float)    Test split percentage

	outputs:
	train_mask  (ndarray) Binary mask containing 1 at positions corresponding to nodes to train on
	val_mask    (ndarray) Binary mask containing 1 at positions correpsonding to nodes to validate on
	test_mask   (ndarray) Binary mask containing 1 at positions correpsonding to nodes to test on
	'''
	def run(self):
		G, node_map = None, None
		with open(os.path.join(self.cfg.output_dir, self.cfg.graph), 'r') as file:
			G = nx.Graph(json.load(file))

		with open(os.path.join(self.cfg.output_dir, self.cfg.node_map), 'r') as file:
			node_map = {k: v for k, v in json.load(file).items()}

		'''
		nx.relabel is converting keys in adjacency list to string integer ids and edges to integer ids,
		so need the second nx.relabel_nodes to convert all nodes to interger ids. If don't do this, then
		there was be a doubling of nodes.
		'''
		G = nx.relabel_nodes(G, node_map)
		G = nx.relabel_nodes(G, {i: int(i) for i in G.nodes()})

		num_nodes = len(G.nodes())
		train_mask = np.zeros(num_nodes)
		test_mask = np.zeros(num_nodes)

		# create training mask from largest connected component
		for i in max(nx.connected_components(G), key=len):
			train_mask[int(i)] = 1

		val_mask = 1-train_mask

		orig = sum(val_mask)

		if self.cfg.test_split:
			val_idx = np.where(val_mask == 1)[0]
			test_idx = np.random.choice(val_idx, int(self.cfg.test_split*num_nodes))
			test_mask[test_idx] = 1
			val_mask[test_idx] = 0

		assert sum(train_mask) + sum(val_mask) + sum(test_mask) == num_nodes

		task_dir = os.path.join(self.cfg.output_tasks_dir, self.__class__.__name__)
		make_dir(task_dir)

		self.save(train_mask.tolist(), os.path.join(task_dir, self.cfg.train_mask))
		self.save(val_mask.tolist(), os.path.join(task_dir, self.cfg.val_mask))
		self.save(test_mask.tolist(), os.path.join(task_dir, self.cfg.test_mask))