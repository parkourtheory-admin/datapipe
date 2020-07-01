'''
Luigi tasks
'''
import os
import json
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

        # default configuration
        default = cfg['DEFAULT']
        self.whitelist = self.get_whitelist() if default.getboolean('whitelist') else []
        self.parallel = default.getboolean('parallel')
        self.pipe = default['pipe']

        # move configuration
        move = cfg['moves']
        self.move_csv = move['csv']

        # video configuration
        video = cfg['videos']
        self.video_src = video['src']
        self.video_dst = video['dst']
        self.video_csv = video['csv']
        self.video_csv_out = video['csv_out']
        self.video_height = video['height']
        self.video_width = video['width']

        if not os.path.exists(video['dst']):
            os.makedirs(video['dst'])
            
        # thumbnail configuration
        thumb = cfg['thumbnails']
        self.thumb_height = thumb['height']
        self.thumb_width = thumb['width']
        self.thumb_dst = thumb['dst']
        
        if not os.path.exists(thumb['dst']):
            os.makedirs(thumb['dst'])


    '''
    Open and return white list

    outputs:
    ids (list) List of ids to whitelist
    '''
    def get_whitelist(self):
        cfg = configparser.ConfigParser()
        cfg.read(is_config('whitelist'))
        return [int(i) for i in cfg['DEFAULT']['ids'].split(',')]

    
class DataCheck(object):

    '''
    inputs:
    config (Configuration) Object containing parsed configuration values
    '''
    def __init__(self, config):
        self.cfg = config
        

    def run(self):
        log = {}
        
        # check over move table
        src = self.cfg.move_csv
        df = pd.read_csv(src, header=0)

        dc = dck.DataCheck(whitelist=self.cfg.whitelist)

        ids = dc.invalid_ids(df)
        log['invalid_ids'] = ids

        edges = dc.find_duplicate_edges(df)
        log['duplicate_edges'] = edges

        dup = dc.duplicated('name', df)
        log['duplicate_nodes'] = dup.to_json()

        err = dc.check_symmetry(df)
        log['symmetry'] = err

        cols = ['id', 'name', 'type', 'desc']
        log['incomplete'] = dc.fine_all_empty(df, cols)

        df = dc.sort_edges(df)
        df = dc.remove_unnamed(df)
        df.to_csv(src, index=False)

        errs = dc.check_type(df)
        log['move_types'] = errs

        write('data_check.json', log)


class CollectVideos(object):
    def __init__(self, config):
        self.cfg = config


    def run(self):
        log = {}
        df = pd.read_csv(self.csv, header=0)

        una, miss, cta = clt.find_missing(self.cfg.move_csv, self.cfg.video_csv, self.cfg.video_csv_out)
    
        log['missing'] = {
            'unavailable': len(una),
            'missing': len(miss),
            'call_to_action': len(cta)
        }

        miss.to_csv(os.path.join(self.cfg.video_csv_out, 'missing.csv'))
        una, found = clt.collect(miss, self.cfg.video_dst, self.cfg.video_csv_out)

        log['collect'] = {
            'unavailable': len(una),
            'found': len(found)
        }

        updated = clt.update_videos(self.cfg.video_csv, found, self.cfg.video_src,
                                    os.path.join(self.cfg.video_csv_out, 'updated.csv'))

        write('collect_videos.json', log)


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
        df.to_csv(dst, index=False, sep='\t')


class FixExtensions(object):
    def __init__(self, config):
        self.cfg = config


    def run(self):
        clt.fix_extensions(self.cfg.video_src)


'''
Format argparse configuration file parameter

inputs:
c (str) Configuration file name

outputs:
(str) formatted file path
'''
def config_path(c):
    if not c.endswith('.ini'):
        c = c.split('.')[0]+'.ini'
    return os.path.join('configs', c)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=config_path, help='Configuration file (available: production, test)')
    args = parser.parse_args()

    cfg = Configuration(args.config)

    # dynamically build pipeline
    pipe = [globals()[task](cfg) for task in cfg.pipe.split(', ')]

    if cfg.parallel:
        pipe = [mp.Process(target=t.run) for t in pipe]
        for t in pipe: t.start()
        for t in pipe: t.join()
    else: 
        for t in pipe: t.run()


if __name__ == '__main__':
    main()