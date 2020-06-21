'''
Steps:
1. Generate sitemap
2. Validate at https://www.xml-sitemaps.com/validate-xml-sitemap.html
'''
import json
import pandas as pd
from datetime import datetime as dt


def sitemap(data):
	today = dt.now().strftime('%Y-%m-%d')
	urls = []

	open_tag = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
	<urlset
	\txmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\"
	\txmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"
	\txsi:schemaLocation=\"http://www.sitemaps.org/schemas/sitemap/0.9\n \
	\thttp://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd\">\n"""

	urls.append(open_tag)

	for i in data:
		i = i.replace(' ', '%20')
		i = '/'.join(['http://www.parkourtheory.com', i.strip('/')])

		urls.append(f"""
		<url>
		\t<loc>{ i }</loc>
		\t<lastmod>{ today }</lastmod>
		\t<changefreq>monthly</changefreq>
		</url>""")

	close_tag = '\n</urlset>'
	urls.append(close_tag)

	return ''.join(urls) 


def main():
	moves = pd.read_csv('moves.tsv', sep='\t')
	data = ['/m/'+m for m in moves['name'].tolist()]
	
	with open('sites.json') as file:
		sites = json.load(file)
		data.extend(sites['sites'])

	with open('sitemap.xml', 'w') as file:
		file.write(sitemap(data))

if __name__ == '__main__':
	main()