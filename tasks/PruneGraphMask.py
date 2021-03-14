'''
Generate training mask for moves with videos, and validation and testing for nodes without videos.
Only need for training if using video features.
'''
import os
import pandas as pd
import numpy as np
from tqdm import tqdm

class PruneGraphMask(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		videos = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')

		df = pd.merge(moves, videos, on='id')
		train_mask = df['link'].notnull()
		validation_mask = df['link'].isnull()
		test_mask = np.zeros(len(df))

		# create test mask from validation_mask
		val_idx = [i for i, item in enumerate(validation_mask) if item]
		test_idx = np.random.choice(val_idx, len(val_idx)//2, replace=False)

		val_mask = validation_mask.to_numpy(dtype=float)
		test_mask[test_idx] = 1
		val_mask[test_idx] = 0

		val_mask = pd.Series(val_mask, dtype=bool)
		test_mask = pd.Series(test_mask, dtype=bool)

		train_size = len(df[train_mask])
		val_size = len(df[val_mask])
		test_size = len(df[test_mask])
		total = train_size+val_size+test_size

		# check that splits match total number of nodes
		assert total == len(df)

		train_mask.to_csv(os.path.join(self.cfg.output_dir, self.cfg.train_mask), sep='\t', index=False)
		val_mask.to_csv(os.path.join(self.cfg.output_dir, self.cfg.val_mask), sep='\t', index=False)
		test_mask.to_csv(os.path.join(self.cfg.output_dir, self.cfg.test_mask), sep='\t', index=False)