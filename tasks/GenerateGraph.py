'''
Generate graph and save.
'''
import os
import json
import pandas as pd
import networkx as nx
from preproc import relational as rel

class GenerateGraph(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		G = rel.dataframe_to_graph(moves)

		with open(os.path.join(self.cfg.video_csv_out, 'adjlist.json'), 'w') as file:
			json.dump(nx.to_dict_of_lists(G), file, ensure_ascii=False, indent=4)
