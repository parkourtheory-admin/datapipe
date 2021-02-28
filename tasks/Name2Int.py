'''
Generate move to id map. Need for relabeling graph nodes before converting from Networkx to DGL.
DGL will automatically relabel the nodes with consecutive integers starting from zero.
Unclear which node will be relabeled first and what order it will proceed in, so should use this map so can consistently
convert between data structures.

If need to convert int id to node string name, when converting from DGL to Networkx, then can simply use name2int.json and 
invert the dictionary.
'''
import os
import json
import pandas as pd

class Name2Int(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		df = None
		with open(self.cfg.move_csv, 'r') as file:
			df = pd.read_csv(file, sep='\t', header=0)
		
		name2int = {n:i for i, n in enumerate(df['name'].tolist())}

		with open(os.path.join(self.cfg.video_csv_out, 'name2int.json'), 'w') as file:
			json.dump(name2int, file, ensure_ascii=False, indent=4)
