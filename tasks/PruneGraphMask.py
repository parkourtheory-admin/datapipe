'''
Create masks of nodes that should be pruned
'''
import os
import sys
import json
import math
import numpy as np
import pandas as pd
import networkx as nx
from networkx.readwrite import json_graph

from tqdm import tqdm
from preproc import relational as rel

class PruneGraphMask(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		videos = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		start_len = len(moves)
		G = rel.dataframe_to_graph(moves)

		# create validation and test sets using moves that do not have videos
		# note: this will not always be true when the video table is complete
		df = pd.merge(moves, videos, on='id')
		train_mask = df['link'].notnull()
		validation_mask = df['link'].isnull()
		test_mask = None

		test_split = self.cfg.test_split
		val_idx = [i for i, item in enumerate(validation_mask) if item]
		test_idx = np.random.choice(val_idx, int(test_split*len(df)))
		val_mask = np.array(validation_mask.tolist(), dtype=float)
		test_mask = np.zeros(len(val_mask))
		test_mask[test_idx] = 1
		val_mask -= test_mask
		val_mask = pd.Series(val_mask, dtype=bool)
		test_mask = pd.Series(test_mask, dtype=bool)

		print(type(train_mask))
		print(type(val_mask))
		print(type(test_mask))

		train_size = len(df[train_mask])
		val_size = len(df[val_mask])
		test_size = len(df[test_mask])
		total = train_size+val_size+test_size

		assert total == len(df)
		print(f'train mask: {train_size}\tvalidation mask: {val_size}\ttest mask: {test_size}\ttotal: {total}')
	
		train_mask.to_csv(os.path.join(self.cfg.video_csv_out, 'train_mask.tsv'), sep='\t', index=False)
		val_mask.to_csv(os.path.join(self.cfg.video_csv_out, 'validation_mask.tsv'), sep='\t', index=False)
		test_mask.to_csv(os.path.join(self.cfg.video_csv_out, 'test_mask.tsv'), sep='\t', index=False)


		with open(os.path.join(self.cfg.video_csv_out, 'adjlist'), 'w') as file:
			json.dump(nx.to_dict_of_lists(G), file, ensure_ascii=False, indent=4)