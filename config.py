'''
'''
import os
import configparser

from utils import is_config

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