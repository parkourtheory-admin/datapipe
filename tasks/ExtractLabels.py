'''
Get type labels
'''
import os
import json
import pandas as pd

class ExtractLabels(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		labels = {m[0]: str(m[1]).split('/') for m in zip(list(moves['name']), list(moves['type']))}
		
		with open(os.path.join(self.cfg.video_csv_out, 'labels.json'), 'w') as file:
			json.dump(labels, file)