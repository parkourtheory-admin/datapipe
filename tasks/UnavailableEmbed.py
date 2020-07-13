'''
Change embed of all moves without videos back to 'unavailable.mp4'.
Basically the reverse of FixEmbed task.
'''
import os
import pandas as pd

class UnavailableEmbed(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		df = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')

		for i, row in df.iterrows():
			if not os.path.exists(os.path.join(self.cfg.video_src, row['embed'])):
				df.loc[i, 'embed'] = 'unavailable.mp4'

		file = os.path.join(self.cfg.video_csv_out, 'video.tsv')
		df.to_csv(file, index=False, sep='\t')