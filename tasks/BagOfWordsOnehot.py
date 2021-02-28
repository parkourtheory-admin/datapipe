'''
Bag of words
'''
import os
import json
import pandas as pd
from collections import defaultdict

class BagOfWordsOnehot(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		df = pd.read_csv(self.cfg.move_csv, sep='\t', header=0)
		features = defaultdict(list)

		# build bag-of-words map
		terms = set(term for move in df['name'] for term in move.split())
		term2index = {term: i for i, term in enumerate(list(terms))}

		# single label classification. this is also the ordering of masks.
		type2id = {str(m):i for i, m in enumerate(list(set(df['type'])))}
		
		for i, sample in enumerate(zip(df['name'], df['type'])):
			move, type_ = sample
			bag = [0]*len(term2index)
			for term in move.split(): bag[term2index[term]] += 1

			features[i] = (move, bag, type2id[str(type_)])
		
		desc = 'One-hot classification of move types using bag-of-words of move names as features.'

		with open(os.path.join(self.cfg.video_csv_out, 'bag-of-words.json'), 'w') as file:
			json.dump({'task': 'onehot', 'features': features, 'label_map': type2id, 'desc': desc}, file, ensure_ascii=False, indent=4)