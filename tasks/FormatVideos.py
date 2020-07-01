'''
'''
import os
import pandas as pd
import multiprocessing as mp
from more_itertools import chunked

from preproc import video as vid

class FormatVideos(object):
    def __init__(self, config):
        self.cfg = config


    def run(self):
        df = pd.read_csv(self.cfg.video_csv, header=0)
        v = vid.Video()

        for block in chunked(df.iterrows(), mp.cpu_count()):
            procs = []

            for row in block:
                video = row[1]['embed']
                file = os.path.join(self.video_src, video)

                procs.append(mp.Process(target=v.resize, 
                             args=(self.cfg.video_height, self.cfg.video_width, file, os.path.join(self.cfg.video_src, video))))

            for p in procs: p.start()
            for p in procs: p.join()