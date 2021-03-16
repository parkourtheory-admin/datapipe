'''
Generate random training, validation, and testing masks.
'''
import os
import json
import numpy as np
import pandas as pd
import networkx as nx

from utils import *

class RandomMasks(object):
	def __init__(self, config):
		self.cfg = config


	'''
	Generate random binary masks for training and validation

	inputs:
	num_nodes   (int)   		 Number of nodes in the entire dataset
	train_split (float) 		 Training split percentage
	val_split   (float) 		 Validation split percentage
	test_split  (float) 		 Test split percentage
	as_list     (bool, optional) If True, return as list else ndarry

	outputs:
	train_mask (ndarray) Binary mask containing 1 at positions corresponding to nodes to train on
	val_mask   (ndarray) Binary mask containing 1 at positions correpsonding to nodes to validate on
	test_mask  (ndarray) Binary mask containing 1 at positions correpsonding to nodes to test on
	'''
	def get_random_mask(self, num_nodes, train_split, val_split, test_split, as_list=False):
		if not all(0 <= i <= 1 for i in [train_split, val_split, test_split]):
			raise Exception('splits must be >= 0  and <= 1')

		if train_split + val_split + test_split != 1:
			raise Exception('splits must sum to 1')

		train_mask = np.zeros(num_nodes)
		train_mask[:int(num_nodes*train_split)] = 1
		np.random.shuffle(train_mask)
		val_mask = np.logical_not(train_mask).astype(int)

		val_idx = [i for i, item in enumerate(val_mask) if item]
		test_idx = np.random.choice(val_idx, int(test_split*num_nodes))

		val_mask = np.array(val_mask.tolist(), dtype=float)
		test_mask = np.zeros(num_nodes)

		test_mask[test_idx] = 1
		val_mask[test_idx] = 0

		assert num_nodes == sum(map(sum, [train_mask, val_mask, test_mask]))

		if as_list:
			return train_mask.tolist(), val_mask.tolist(), test_mask.tolist()

		return train_mask, val_mask, test_mask


	def run(self):
		task_dir = os.path.join(self.cfg.output_tasks_dir, self.__class__.__name__)
		save_path = lambda path: os.path.join(task_dir, path)

		with open(os.path.join(self.cfg.output_tasks_dir, 'GenerateGraph', self.cfg.graph), 'r') as file:
			G = nx.Graph(json.load(file))

			train_mask, val_mask, test_mask = self.get_random_mask(len(G), self.cfg.train_split, self.cfg.val_split, self.cfg.test_split, as_list=True)

			train_mask = pd.Series(train_mask, dtype=bool)
			val_mask = pd.Series(val_mask, dtype=bool)
			test_mask = pd.Series(test_mask, dtype=bool)

			train_mask.to_csv(save_path(self.cfg.train_mask), sep='\t', index=False)
			val_mask.to_csv(save_path(self.cfg.val_mask), sep='\t', index=False)
			test_mask.to_csv(save_path(self.cfg.test_mask), sep='\t', index=False)