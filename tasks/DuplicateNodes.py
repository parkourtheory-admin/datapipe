'''
TODO:
Refer to https://github.com/parkourtheory-admin/datapipe/issues/67
'''
import pandas as pd

from utils import write
from preproc import relational as rel

class DuplicateNodes(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		pass