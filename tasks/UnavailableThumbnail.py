'''
Should only need to run once ever and store the thumbnail.
'''
import os
from preproc import video as vid

from utils import write

class UnavailableThumbnail(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		v = vid.Video()
		res = {}
		embed = 'unavailable'
		file = f'{embed}.mp4'
		v.thumbnail(res, os.path.join(self.cfg.video_src, file))
		write(os.path.join(self.cfg.video_csv_out, embed+'.json'), res)