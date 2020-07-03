'''
'''
import pandas as pd
from stats import label_dist
from utils import write

class DataStats(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		ml = label_dist(moves, single=False)
		sl = label_dist(moves, single=True)
		
		write('multi_type_dist.json', ml)
		write('single_type_dist.json', sl)

		print(f'multi-label: {len(ml)}\tsingle-label: {len(sl)}')
