'''
Bag of words
'''

import os
import json
import pandas as pd
from collections import defaultdict

class BagOfWords(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		file = os.path.join(self.cfg.video_csv_out, 'pruned_moves.tsv')
		df = pd.read_csv(file, sep='\t', header=0)

		features = defaultdict(list)
		# build bag-of-words map
		terms = set(term for move in df['name'] for term in move.split())
		term2index = {term: i for i, term in enumerate(list(terms))}

		for move in df['name']:
			bag = [0]*len(term2index)
			for term in move.split():
				bag[term2index[term]] += 1
			features[move] = bag

		with open(os.path.join(self.cfg.video_csv_out, 'bag-of-words.json'), 'w') as file:
			json.dump(features, file, ensure_ascii=False, indent=4)