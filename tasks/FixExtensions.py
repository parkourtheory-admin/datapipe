'''
'''
from collect import collector as clt

class FixExtensions(object):
    def __init__(self, config):
        self.cfg = config


    def run(self):
        clt.fix_extensions(self.cfg.video_src)