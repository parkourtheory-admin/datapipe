'''
Create masks of nodes that should be pruned
'''
import os
import sys
import json
import math
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

		df = pd.merge(moves, videos, on='id')
		train_mask = df['embed'].notnull()
		validation_mask = df['embed'].isnull()

		train_size = len(df[train_mask])
		val_size = len(df[validation_mask])
		print(f'train mask: {train_size}\tvalidation mask: {val_size}\ttotal: {train_size+val_size}')
	
		train_mask.to_csv(os.path.join(self.cfg.video_csv_out, 'train_mask.tsv'), sep='\t', index=False)
		validation_mask.to_csv(os.path.join(self.cfg.video_csv_out, 'validation_mask.tsv'), sep='\t', index=False)