'''
If properties are added to or removed from configuration files, change this object to make
them available to all tasks.
'''
import os
import configparser

from utils import *

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
        self.video_height = int(video['height'])
        self.video_width = int(video['width'])
            
        # thumbnail configuration
        thumb = cfg['thumbnails']
        self.thumb_height = int(thumb['height'])
        self.thumb_width = int(thumb['width'])
        self.thumb_dst = thumb['dst']
        
        make_dirs(video['csv_out'])
        make_dirs(video['src'])
        make_dirs(video['dst'])
        make_dirs(thumb['dst'])


    '''
    Open and return white list

    outputs:
    ids (list) List of ids to whitelist
    '''
    def get_whitelist(self):
        cfg = configparser.ConfigParser()
        cfg.read(is_config('whitelist'))
        return [int(i) for i in cfg['DEFAULT']['ids'].split(',')]