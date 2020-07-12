'''
'''
import os
import pandas as pd
from collect import collector as clt
from utils import write

class RenameVideos(object):
    def __init__(self, config):
        self.cfg = config


    def run(self):
        moves = pd.read_csv(self.cfg.move_csv, header=0, sep='\t')
        videos = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
        df = pd.merge(moves, videos, on='id')

        df, err = clt.update_embed(df, self.cfg.video_src)
        df = df.drop(list(moves)[1:], axis=1)
        df.to_csv(self.cfg.video_csv, index=False, sep='\t')
        write('no_videos_rename.txt', err)