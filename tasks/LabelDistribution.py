'''
Label distribution of move table
'''
import os
from collections import OrderedDict, defaultdict

import pandas as pd
from stats import label_dist
from utils import write

import matplotlib.pyplot as plt
from preproc import relational as rel
from pprint import pprint

class LabelDistribution(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		multi_hot = label_dist(moves, multihot=True)
		one_hot = label_dist(moves, multihot=False)
		multi_hot_percentages = label_percentages(multi_hot)
		one_hot_percentages = label_percentages(one_hot)
		
		write('multi_hot_dist.json', multi_hot)
		write('single_hot_dist.json', one_hot)
		write('multi_hot_percentages.json', multi_hot_percentages)
		write('one_hot_percentages.json', one_hot_percentages)
		multi_hot = OrderedDict((k, v) for k, v in sorted(multi_hot.items(), key=lambda x: x[1]))
		one_hot = OrderedDict((k, v) for k, v in sorted(one_hot.items(), key=lambda x: x[1]))

		self.plot(multi_hot.keys(), multi_hot.values(), 'labels', 'frequency', 'Multi-hot Labels', labelsize=8)
		self.plot(one_hot.keys(), one_hot.values(), 'labels', 'frequency', 'One-hot Labels')


	def plot(self, x, y, xlabel, ylabel, title, labelsize=10):
		plt.figure(figsize=(20,10))
		plt.bar(x, y)
		plt.tick_params(axis='x', which='major', labelsize=labelsize)
		plt.xticks(rotation=45, ha='right')
		plt.xlabel('labels')
		plt.ylabel(ylabel)
		plt.title(title)
		plt.subplots_adjust(left=0.1, bottom=0.3)
		plt.savefig(os.path.join(self.cfg.output_dir, f'{title}.pdf'))
		plt.show()
		