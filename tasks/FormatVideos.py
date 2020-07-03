'''
'''
import os
import pandas as pd
import multiprocessing as mp
from more_itertools import chunked

from utils import accuracy, write
from preproc import video as vid

class FormatVideos(object):
    def __init__(self, config):
        self.cfg = config


    def run(self):
        df = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
        v = vid.Video()
        res = mp.Manager().dict()

        for block in chunked(df.iterrows(), mp.cpu_count()):
            procs = []

            for row in block:
                video = row[1]['embed']
                file = os.path.join(self.cfg.video_src, video)
                args = (self.cfg.video_height, 
                        self.cfg.video_width, 
                        file, 
                        os.path.join(self.cfg.video_src, video),
                        res)
                procs.append(mp.Process(target=v.resize, args=args))

            for p in procs: p.start()
            for p in procs: p.join()

        failed = list(filter(lambda x: not x, res.values()))
        accuracy(failed, res)
        write('format_video.json', res)

