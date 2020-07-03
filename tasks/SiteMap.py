'''
Steps:
1. Generate sitemap
2. Validate at https://www.xml-sitemaps.com/validate-xml-sitemap.html
'''

import pandas as pd

class SiteMap(object):
	def __init__(self, config):
		self.cfg = config


	def run(self):
		moves = pd.read_csv(self.cfg.move_csv, sep='\t')
		data = ['/m/'+m for m in moves['name'].tolist()]
		
		with open('sites.json') as file:
			sites = json.load(file)
			data.extend(sites['sites'])

		with open('sitemap.xml', 'w') as file:
			file.write(sitemap(data))