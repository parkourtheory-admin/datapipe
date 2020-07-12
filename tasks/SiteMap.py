'''
Steps:
1. Generate sitemap
2. Validate at https://www.xml-sitemaps.com/validate-xml-sitemap.html
'''
import json
import pandas as pd

from sitemap import sitemap
from utils import write

class SiteMap(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, sep='\t')
		data = ['/m/'+m for m in moves['name'].tolist()]
		
		with open('sitemap/sites.json') as file:
			sites = json.load(file)
			data.extend(sites['sites'])

		write('sitemap.xml', sitemap(data))