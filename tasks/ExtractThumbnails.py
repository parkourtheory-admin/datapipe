'''
'''
import os
import pandas as pd

from preproc import video as vid
from collect import collector as clt

class ExtractThumbnails(object):
    def __init__(self, config):
        self.cfg = config


    def run(self):
        df = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
        v = vid.Video()
        res = v.extract_thumbnails(self.cfg.video_src, 300, 168)

        df, err = clt.update_thumbnail(df, res)
        err.to_csv(os.path.join(self.cfg.video_csv_out, 'missing_thumbnails.tsv'), sep='\t')

        dst = os.path.join(self.cfg.video_csv_out, 'updated.tsv')
        df.to_csv(self.cfg.video_csv_out, index=False, sep='\t')