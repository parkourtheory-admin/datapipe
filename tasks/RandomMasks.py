'''
Generate random training, validation, and testing masks.
'''
import os
import numpy as np

class RandomMasks(object):
	def __init__(self, config):
		self.cfg = config


	'''
	Generate random binary masks for training and validation

	inputs:
	num_nodes   (int)   Number of nodes in the entire dataset
	train_split (float) Training split percentage
	val_split   (float) Validation split percentage
	test_split  (float) Test split percentage

	outputs:
	train_mask (ndarray) Binary mask containing 1 at positions corresponding to nodes to train on
	val_mask   (ndarray) Binary mask containing 1 at positions correpsonding to nodes to validate on
	test_mask  (ndarray) Binary mask containing 1 at positions correpsonding to nodes to test on
	'''
	def get_random_mask(self, num_nodes, train_split, val_split, test_split):
		if not all(0 <= i <= 1 for i in [train_split, val_split, test_split]):
			raise Exception('splits must be >= 0  and <= 1')

		if train_split + val_split + test_split != 1:
			raise Exception('splits must sum to 1')

		train_mask = np.zeros(num_nodes)
		train_mask[:int(num_nodes*split)] = 1
		np.random.shuffle(train_mask)
		val_mask = train_mask - 1

		val_idx = [i for i, item in enumerate(train_mask-1) if item]
		test_idx = np.random.choice(val_idx, int(test_split*num_nodes))

		val_mask = np.array(val_mask.tolist(), dtype=float)
		test_mask = np.zeros(num_nodes)

		test_mask[test_idx] = 1
		val_mask -= test_mask

		return train_mask, val_mask, test_mask


	def save(self, data, save_path):
		with open(save_path, 'w') as file:
			json.dump(data, file, ensure_ascii=False, indent=4)


	def run(self):
		with open(os.path.join(self.cfg.video_csv_out, 'adjlist'), 'r') as file:
			G = nx.Graph(json.load(file))

			train_mask, val_mask, test_mask = self.get_random_mask(len(G), self.train_split, self.val_split, self.test_split)
			
			self.save(train_mask, os.path.join(self.cfg.video_csv_out, 'train_mask.json'))
			self.save(val_mask, os.path.join(self.cfg.video_csv_out, 'val_mask.json'))
			self.save(test_mask, os.path.join(self.cfg.video_csv_out, 'test_mask.json'))