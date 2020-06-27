'''
Luigi tasks
'''
import os
import json
import luigi
import configparser
import threading as th
import multiprocessing as mp
from more_itertools import chunked

import pandas as pd

from validate import datacheck as dck
from collect import collector as clt
from preproc import video as vid
from utils import write, is_config


class PATHTask(object):
	def __init__(self):
		cfg = configparser.ConfigParser()
		cfg.read('configs/test.ini')

		self.default = cfg['DEFAULT']
		self.move_pipe = cfg['moves']
		self.video_pipe = cfg['videos']
		self.dst = self.video_pipe['dst']
		self.file = self.video_pipe['csv']
		self.whitelist = self.get_whitelist() if self.default.getboolean('whitelist') else []

		self.vid_dir = os.path.join(self.dst, 'video')
		self.img_dir = os.path.join(self.dst, 'thumbnail')

		if not os.path.exists(self.dst) or len(self.dst) == 0:
			os.makedirs(self.dst)
			os.makedirs(self.vid_dir)
			os.makedirs(self.img_dir)



	'''
	Open and return white list

	outputs:
	ids (list) List of ids to whitelist
	'''
	def get_whitelist(self):
		cfg = configparser.ConfigParser()
		cfg.read(is_config('whitelist'))
		return [int(i) for i in cfg['DEFAULT']['ids'].split(',')]

	
class DataCheck(luigi.Task, PATHTask):
	def __init__(self):
		luigi.Task.__init__(self)
		PATHTask.__init__(self)


	def requires(self):
		pass


	def output(self):
		return luigi.LocalTarget('data_check.json')


	def run(self):
		src = self.move_pipe['csv']
		df = pd.read_csv(src, header=0)

		log = {}
		dc = dck.DataCheck(whitelist=self.whitelist)
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


class CollectVideos(luigi.Task, PATHTask):
	def __init__(self):
		luigi.Task.__init__(self)
		PATHTask.__init__(self)


	def requires(self):
		pass


	def output(self):
		pass


	def run(self):
		log = {}
		df = pd.read_csv(video_pipe['csv'], header=0)

		una, miss, cta = clt.find_missing(moves_path, videos_path, csv_out)
	
		log['missing'] = {
			'unavailable': len(una),
			'missing': len(miss),
			'call_to_action': len(cta)
		}

		miss.to_csv(os.path.join(csv_out, 'missing.csv'))
		una, found = clt.collect(miss, dst, csv_out)

		log['collect'] = {
			'unavailable': len(una),
			'found': len(found)
		}

		clt.update_videos(videos_path, found, save_path)

		write('collect_videos.json', log)


class FormatVideos(luigi.Task, PATHTask):
	def __init__(self):
		luigi.Task.__init__(self)
		PATHTask.__init__(self)


	def requires(self):
		pass


	def output(self):
		pass


	def run(self):
		df = pd.read_csv(video_pipe['csv'], header=0)
		v = vid.Video()

		for block in chunked(df.iterrows(), mp.cpu_count()):
			procs = []

			for row in block:
				video = row[1]['embed']
				file = os.path.join(src_dir, video)

				procs.append(mp.Process(target=v.resize, 
							 args=(height, width, file, os.path.join(vid_dir, video))))

			for p in procs: p.start()
			for p in procs: p.join()


class ExtractThumbnails(luigi.Task, PATHTask):

	def requires(self):
		pass


	def output(self):
		pass


	def run(self):
		df = pd.read_csv(video_pipe['csv'], header=0)
		v = vid.Video()

		for block in chunked(df.iterrows(), mp.cpu_count()):
			threads = []

			for row in block:
				video = row[1]['embed']
				thumbnail = video.split('.')[0]+'png'
				file = os.path.join(src_dir, video)

				threads.append(th.Thread(target=v.thumbnail,
							 args=(height, width, os.path.join(img_dir, thumbnail))))

			for t in threads: t.start()
			for t in threads: t.join()


if __name__ == '__main__':
	dc = DataCheck()
	dc.run()