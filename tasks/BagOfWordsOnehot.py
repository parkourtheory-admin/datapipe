'''
Bag of words
'''
import os
import json
import pandas as pd
from collections import defaultdict

from preproc import relational as rel

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
		filename = 'bag-of-words-onehot-split.json' if self.cfg.is_split else 'bag-of-words-onehot.json'

		with open(os.path.join(self.cfg.output_dir, filename), 'w') as file:
			data = {'task': 'onehot', 'label_map': type2id, 'desc': desc}
			
			train_mask_path = os.path.join(self.cfg.output_dir, self.cfg.train_mask)
			val_mask_path = os.path.join(self.cfg.output_dir, self.cfg.val_mask)
			test_mask_path = os.path.join(self.cfg.output_dir, self.cfg.test_mask)

			train_set, val_set, test_set = rel.split_dataset_on_masks(features, train_mask_path, val_mask_path, test_mask_path)

			if self.cfg.is_split:
				data.update({'train': train_set, 'validation': val_set, 'test': test_set})
			else:	
				data['features'] = features
			
			json.dump(data, file, ensure_ascii=False, indent=4)