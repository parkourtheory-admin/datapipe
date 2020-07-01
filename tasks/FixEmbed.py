'''
Issue #55
https://github.com/parkourtheory-admin/datapipe/issues/55
'''
import os

class FixEmbed(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		files = os.listdir(self.cfg.video_src)
