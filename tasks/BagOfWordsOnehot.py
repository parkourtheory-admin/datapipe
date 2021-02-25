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

		# build bag-of-words map
		terms = set(term for move in df['name'] for term in move.split())
		term2index = {term: i for i, term in enumerate(list(terms))}

		# single label classification
		type2id = {str(m):i for i, m in enumerate(list(set(df['type'])))}

		def process(src):
			features = defaultdict(list)

			for i, sample in enumerate(zip(src['name'], src['type'])):
				move, type_ = sample
				bag = [0]*len(term2index)
				for term in move.split(): bag[term2index[term]] += 1

				features[i] = (move, bag, type2id[str(type_)])

			return features

		def save(features, filename):
			with open(os.path.join(self.cfg.video_csv_out, filename), 'w') as file:
				json.dump({'task': 'onehot', 'features': features}, file, ensure_ascii=False, indent=4)
		
		if self.cfg.is_split:
			train_mask = pd.read_csv(os.path.join(self.cfg.video_csv_out, 'train_mask.tsv'), sep='\t', header=0).to_numpy()
			val_mask = pd.read_csv(os.path.join(self.cfg.video_csv_out, 'validation_mask.tsv'), sep='\t', header=0).to_numpy()
			test_mask = pd.read_csv(os.path.join(self.cfg.video_csv_out, 'test_mask.tsv'), sep='\t', header=0).to_numpy()

			save(process(df[train_mask]), 'train_bag-of-words.json')
			save(process(df[val_mask]), 'val_bag-of-words.json')
			save(process(df[test_mask]), 'test_bag-of-words.json')

		else:
			save(process(df), 'bag-of-words.json')