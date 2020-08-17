'''
This task should only be executed after RenameVideos.
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
        res = v.extract_thumbnails(self.cfg.video_src, self.cfg.thumb_dst, 300, 168)

        files = [i for i in os.listdir(self.cfg.video_src)]
        assert len(res) == len(files), f'videos: {len(files)}\textracted: {len(res)}'

        df, err = clt.update_thumbnail(df, res)
        
        missing_dst = os.path.join(self.cfg.video_csv_out, 'missing_thumbnails.tsv')
        updated_dst = os.path.join(self.cfg.video_csv_out, 'updated.tsv')

        print(f'thumbnails:\nmissing: {len(missing_dst)}\nupdated: {len(updated_dst)}')

        err.to_csv(missing_dst, index=False, sep='\t')
        df.to_csv(updated_dst, index=False, sep='\t')