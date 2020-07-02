'''
Issue #55
https://github.com/parkourtheory-admin/datapipe/issues/55
'''
import os
import pandas as pd

class FixEmbed(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		files = [f.strip() for f in os.listdir(self.cfg.video_src)]
		
		moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
		videos = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
		df = pd.merge(moves, videos, on='id')

		for i, row in df.iterrows():
			f = row['name'].lower().strip().replace(' ', '_')+'.mp4'
			df.at[i, 'embed'] = f if f in files else 'unavailable.mp4'

		df = df.drop(['name', 'prereq', 'subseq', 'type', 'alias', 'description'], axis=1)
		df.to_csv('output/fixed.tsv', index=False, sep='\t')