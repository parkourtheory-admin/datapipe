'''
Prune knowledge graph
'''
import os
import sys
import json
import math
import pandas as pd
import networkx as nx

from tqdm import tqdm
from preproc import relational as rel

class PruneGraph(object):
	def __init__(self, config):
		self.cfg = config


	def remove_from_edge(self, df, move, edge, tgt):
		row = df[df['name'] == move]
		rel = row[edge].squeeze()

		if isinstance(rel, str):
			rel = rel.split(', ')
			if tgt in rel:
				rel.pop(rel.index(tgt))
				df.loc[df['name'] == move, [edge]] = ', '.join(rel) if len(rel) > 0 else math.nan 


	def run(self):
		videos = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		start_len = len(moves)
		G = rel.dataframe_to_graph(moves)

		df = pd.merge(moves, videos, on='id')
		df = df[df['embed'].isnull()]
		missing_moves = list(df['name'])

		for move in tqdm(missing_moves):
			for neighbor in G.neighbors(move):
				self.remove_from_edge(moves, neighbor, 'prereq', move)
				self.remove_from_edge(moves, neighbor, 'subseq', move)

			index = moves[moves['name'] == move].index
			moves.drop(index=index, inplace=True)
			videos.drop(index=index, inplace=True)

		end_len = len(moves)

		# create new graph of moves from pruned dataframes
		G = rel.dataframe_to_graph(moves, validate=True)

		# check that missing moves are not in the graph to validate pruning worked
		errors = []
		for move in missing_moves:
			if G.has_node(move): errors.append(move)

		assert len(errors) == 0

		G = nx.to_dict_of_lists(G)

		print(f'total: {start_len}\tmissing: {len(missing_moves)}\tpruned: {start_len - end_len}\tremaining: {end_len} moves\tnodes: {len(G)}')
	
		df[['id', 'name']].to_csv(os.path.join(self.cfg.video_csv_out, 'pruned.tsv'), sep='\t', index=False)
		moves.to_csv(os.path.join(self.cfg.video_csv_out, 'moves.tsv'), sep='\t', index=False)
		videos.to_csv(os.path.join(self.cfg.video_csv_out, 'videos.tsv'), sep='\t', index=False)

	
		with open(os.path.join(self.cfg.video_csv_out, 'adjlist'), 'w') as file:
			json.dump(G, file, ensure_ascii=False, indent=4)
