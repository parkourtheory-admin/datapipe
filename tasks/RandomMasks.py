'''
Generate random training, validation, and testing masks.
'''
import os
import json
import numpy as np
import networkx as nx

import sys

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
	as_list     (bool, optional) If True, return as list

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
		val_mask = train_mask - 1

		val_idx = [i for i, item in enumerate(train_mask-1) if item]
		test_idx = np.random.choice(val_idx, int(test_split*num_nodes))

		val_mask = np.array(val_mask.tolist(), dtype=float)
		test_mask = np.zeros(num_nodes)

		test_mask[test_idx] = 1
		val_mask -= test_mask

		if as_list:
			return train_mask.tolist(), val_mask.tolist(), test_mask.tolist()

		return train_mask, val_mask, test_mask


	def save(self, data, save_path):
		with open(save_path, 'w') as file:
			json.dump(data, file, ensure_ascii=False, indent=4)


	def run(self):
		with open(os.path.join(self.cfg.output_dir, self.cfg.graph), 'r') as file:
			G = nx.Graph(json.load(file))

			train_mask, val_mask, test_mask = self.get_random_mask(len(G), self.cfg.train_split, self.cfg.val_split, self.cfg.test_split, as_list=True)

			self.save(train_mask, os.path.join(self.cfg.output_dir, 'train_mask.json'))
			self.save(val_mask, os.path.join(self.cfg.output_dir, 'val_mask.json'))
			self.save(test_mask, os.path.join(self.cfg.output_dir, 'test_mask.json'))