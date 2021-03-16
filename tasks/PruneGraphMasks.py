'''
Generate training mask for moves with videos, and validation and testing for nodes without videos.
Only need for training if using video features.
'''
import os
import pandas as pd
import numpy as np
from tqdm import tqdm

from utils import *

class PruneGraphMasks(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		task_dir = os.path.join(self.cfg.output_tasks_dir, self.__class__.__name__)
		save_path = lambda path: os.path.join(task_dir, path)
		
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

		# check that splits match total number of nodes
		assert len(df) == sum(map(sum, [train_mask, val_mask, test_mask]))

		train_mask.to_csv(save_path(self.cfg.train_mask), sep='\t', index=False)
		val_mask.to_csv(save_path(self.cfg.val_mask), sep='\t', index=False)
		test_mask.to_csv(save_path(self.cfg.test_mask), sep='\t', index=False)