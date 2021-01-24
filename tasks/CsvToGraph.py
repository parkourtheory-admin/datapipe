import os
import math
import numbers
import numpy as np
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt

from time import time
from colorama import Fore, Back, Style

from preproc import relational as rel

class CsvToGraph(object):
	def __init__(self, config):
		self.cfg = config


	'''
	Read in a table file

	inputs:
	data_dir (str) Directory containing files
	src 	 (str) Name of file

	outputs:
	df (pd.DataFrame) Read in DataFrame
	'''
	def open_table(self, data_dir, src):

		df = None
		abs_path = os.path.join(data_dir, src)

		if src.endswith('.xls') or src.endswith('.xlsx'):
			df = pd.read_excel(abs_path)
		elif src.endswith('.csv'):
			df = pd.read_csv(abs_path)
		elif src.endswith('.tsv'):
			df = pd.read_csv(abs_path, sep='\t')
		else:
			raise Exception('Invalid file: not a table')

		return df


	def run(self):

		batch_time = time()
		data_dir = 'remaining'
		save_dir = 'output'

		if not os.path.exists(save_dir):
			os.mkdir(save_dir)

		for src in get_files(data_dir, last_index=None):
			single_src_time = time()

			print(f'{src}...')
			df = self.open_table(data_dir, src)
			source = src.split('.')[0]

			# column headers have trailing spaces
			df.rename(columns={c:c.strip() for c in df.columns}, inplace=True)

			G = nx.Graph()

			rel.create_src_node(G, source)
			rel.create_row_nodes(G, df, source)
			rel.create_col_nodes(G, df)
			rel.connect_row_to_col(G, df)

			# Recover original table
			try:
				recovered = rel.graph_to_table(G)
				assert df.equals(recovered)


				# save graph
				nx.write_edgelist(G, f'{save_dir}/{source.lower()}/edgelist.gz')

				print(f'{source} completed')
			except AssertionError:
				print(f'{source} {Fore.RED}failed{Style.RESET_ALL}')

			print(f'{time()-single_src_time} - {src}')
		print(f'total time: {time() - batch_time}')