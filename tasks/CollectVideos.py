'''
'''
import os
import pandas as pd
from collect import collector as clt
from utils import write

class CollectVideos(object):
    def __init__(self, config):
        self.cfg = config


    def run(self):
        log = {}
        df = pd.read_csv(self.csv, header=0, sep='\t')

        una, miss, cta = clt.find_missing(self.cfg.move_csv, self.cfg.video_csv, self.cfg.video_csv_out)
    
        log['missing'] = {
            'unavailable': len(una),
            'missing': len(miss),
            'call_to_action': len(cta)
        }

        miss.to_csv(os.path.join(self.cfg.video_csv_out, 'missing.csv'), sep='\t')
        una, found = clt.collect(miss, self.cfg.video_dst, self.cfg.video_csv_out)

        log['collect'] = {
            'unavailable': len(una),
            'found': len(found)
        }

        updated = clt.update_videos(self.cfg.video_csv, found, self.cfg.video_src,
                                    os.path.join(self.cfg.video_csv_out, 'updated.csv'))

        write('collect_videos.json', log)