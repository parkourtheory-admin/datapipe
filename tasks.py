'''
Luigi tasks
'''
import os
import json
import luigi
import argparse
import configparser
import threading as th
import multiprocessing as mp
from more_itertools import chunked

import pandas as pd

from validate import datacheck as dck
from collect import collector as clt
from preproc import video as vid
from utils import write, is_config


class Configuration(object):
	def __init__(self, config):
		cfg = configparser.ConfigParser()
		cfg.read(config)

		default = cfg['DEFAULT']
		self.whitelist = get_whitelist() if default.getboolean('whitelist') else []
		self.move_pipe = cfg['moves']
		self.video_pipe = cfg['videos']
		self.thumbnail_pipe = cfg['thumbnails']
		dst = video_pipe['dst']

		vid_dir = os.path.join(dst, 'video')
		img_dir = os.path.join(dst, 'thumbnails')

		if not os.path.exists(dst) or len(dst) == 0:
			os.makedirs(dst)
			os.makedirs(vid_dir)
			os.makedirs(img_dir)

		self.dst = dst


	'''
	Open and return white list

	outputs:
	ids (list) List of ids to whitelist
	'''
	def whitelist(self):
		cfg = configparser.ConfigParser()
		cfg.read(is_config('whitelist'))
		return [int(i) for i in cfg['DEFAULT']['ids'].split(',')]

	
class DataCheck(luigi.Task):
	def __init__(self, config):
		luigi.Task.__init__(self)
		self.config = config

	
	def requires(self):
		pass


	def output(self):
		return luigi.LocalTarget('data_check.json')


	def run(self):
		src = self.config.move_pipe['csv']
		df = pd.read_csv(src, header=0)

		log = {}
		dc = dck.DataCheck(whitelist=self.config.whitelist())
		ids = dc.invalid_ids(df)
		log['invalid_ids'] = ids

		edges = dc.find_duplicate_edges(df)
		log['duplicate_edges'] = edges

		dup = dc.duplicated('name', df)
		log['duplicate_nodes'] = dup.to_json()

		adj = dc.get_adjacency(df)
		err = dc.check_symmetry(adj)
		log['symmetry'] = err

		columns = ['id', 'name', 'type', 'desc']
		log['incomplete'] = [{col: dc.find_empty(df, col)} for col in columns]

		dc.sort_edges(df)
		df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
		df.to_csv(src, index=False)

		errs = dc.check_type(df)
		log['move_types'] = errs

		write(self.output(), log)


class CollectVideos(luigi.Task):
	def __init__(self, config):
		luigi.Task.__init__(self)
		video = config.video_pipe
		move = config.move_pipe
		self.move_csv = move['csv']
		self.video_csv = video['csv']
		self.csv_out = video['csv_out']
		self.dst = video['dst']


	def requires(self):
		pass


	def output(self):
		return luigi.LocalTarget('collect_videos.json')


	def run(self):
		log = {}
		df = pd.read_csv(self.csv, header=0)

		una, miss, cta = clt.find_missing(self.move_csv, self.video_csv, self.csv_out)
	
		log['missing'] = {
			'unavailable': len(una),
			'missing': len(miss),
			'call_to_action': len(cta)
		}

		miss.to_csv(os.path.join(self.csv_out, 'missing.csv'))
		una, found = clt.collect(miss, self.dst, self.csv_out)

		log['collect'] = {
			'unavailable': len(una),
			'found': len(found)
		}

		clt.update_videos(self.video_csv, found, os.path.join(self.csv_out, 'updated.csv'))

		write('collect_videos.json', log)


class FormatVideos(luigi.Task):
	def __init__(self, config):
		luigi.Task.__init__(self)
		self.config = config
		video = config.video_pipe
		move = config.move_pipe
		self.video_src = video['src']
		self.height = video['height']
		self.width = video['width']


	def requires(self):
		pass


	def output(self):
		pass


	def run(self):
		df = pd.read_csv(self.video_pipe['csv'], header=0)
		v = vid.Video()

		for block in chunked(df.iterrows(), mp.cpu_count()):
			procs = []

			for row in block:
				video = row[1]['embed']
				file = os.path.join(self.video_src, video)

				procs.append(mp.Process(target=v.resize, 
							 args=(self.height, self.width, file, os.path.join(self.video_src, video))))

			for p in procs: p.start()
			for p in procs: p.join()


class ExtractThumbnails(luigi.Task):
	def __init__(self, config):
		luigi.Task.__init__(self)
		self.config = config
		video = config.video_pipe
		thumb = config.thumbnail_pipe
		self.height = thumb['height']
		self.width = thumb['width']
		self.video_src = video['src']
		self.dst = thumb['dst']


	def requires(self):
		pass


	def output(self):
		pass


	def run(self):
		df = pd.read_csv(self.video['csv'], header=0)
		v = vid.Video()

		for block in chunked(df.iterrows(), mp.cpu_count()):
			threads = []

			for row in block:
				video = row[1]['embed']
				thumbnail = video.split('.')[0]+'png'
				file = os.path.join(self.video_src, video)

				threads.append(th.Thread(target=v.thumbnail,
							 args=(self.height, self.width, os.path.join(self.dst, thumbnail))))

			for t in threads: t.start()
			for t in threads: t.join()


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file (available: production, test)')
	parser.add_argument('--loop', '-l', action='store_true', help='Loop execution (default: False)')
	parser.add_argument('--pipes', '-p', type=is_pipes, nargs='+', required=True, help='Specify pipelines to execute. Required by default. (options: m (move), v (video))')
	args = parser.parse_args()

	config = Configuration(args.config)

	# dynamically build pipeline
	pipe = [globals()[task](config) for task in cfg[name]['pipe'].split(', ')]
	luigi.build(pipe)


if __name__ == '__main__':
	main()