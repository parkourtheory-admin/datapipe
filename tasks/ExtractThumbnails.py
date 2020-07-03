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
        v = vid.Video()
        
        df = pd.read_csv(self.cfg.video_csv, header=0, sep='\t')
        res = v.extract_thumbnails(self.cfg.video_src, 300, 168)

        df, err = clt.update_thumbnail(df, res)
        
        missing_dst = os.path.join(self.cfg.video_csv_out, 'missing_thumbnails.tsv')
        updated_dst = os.path.join(self.cfg.video_csv_out, 'updated.tsv')

        err.to_csv(missing_dst, index=False, sep='\t')
        df.to_csv(updated_dst, index=False, sep='\t')