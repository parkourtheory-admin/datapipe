'''
Site map generation API
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