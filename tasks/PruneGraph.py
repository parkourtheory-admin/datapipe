'''
Prune knowledge graph
'''
import os
import json
import pandas as pd
import networkx as nx

from tqdm import tqdm
from preproc import relational as rel

class PruneGraph(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		videos = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		start_len = len(moves)
		G = rel.dataframe_to_graph(moves)

		df = pd.merge(moves, videos, on='id')
		df = df[df['link'].isnull()]
		df = df[df['embed'].isnull()]
		missing_moves = list(df['name'])
		labels = []

		for move in tqdm(missing_moves):
			for neighbor in G.edges(move):
				row = moves.loc[moves['name'] == neighbor[1]]
				pre, sub = row['prereq'], row['subseq']

				if isinstance(list(pre), str):
					pre = list(pre)[0].split(', ')

					if move in pre:
						pre.pop(pre.index(move))
						moves.loc[moves['name'] == neighbor[1], ['prereq']] = ', '.join(pre)

				if isinstance(list(sub), str):
					sub = list(sub)[0].split(', ')

					if move in sub:
						sub.pop(sub.index(move))
						moves.loc[moves['name'] == neighbor[1], ['subseq']] = ', '.join(sub)

			index = moves[moves['name'] == move].index
			moves.drop(index=index, inplace=True)
			videos.drop(index=index, inplace=True)

		end_len = len(moves)

		print(f'total: {start_len}\tpruned: {start_len - end_len}\tremaining: {end_len} moves')
		
		df[['id', 'name']].to_csv(os.path.join(self.cfg.video_csv_out, 'pruned.tsv'), sep='\t', index=False)
		moves.to_csv(os.path.join(self.cfg.video_csv_out, 'moves.tsv'), sep='\t', index=False)
		videos.to_csv(os.path.join(self.cfg.video_csv_out, 'videos.tsv'), sep='\t', index=False)
		
		adjlist = nx.to_dict_of_lists(G)
		
		with open(os.path.join(self.cfg.video_csv_out, 'pruned_path.adjlist'), 'w') as file:
			json.dump(adjlist, file)
