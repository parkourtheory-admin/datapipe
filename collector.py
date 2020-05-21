import os
import sys
import pandas as pd
from tqdm import tqdm
from pytube import YouTube

sys.path.insert(1, '/media/ch3njus/Seagate4TB/projects/pystagram')

from pystagram import Instagram

class Collector(object):
    def __init__(self, src, dst):
        self.src = src
        self.dst = dst


    def collect(self):
        failed = pd.DataFrame(columns=['id', 'name', 'vid', 'channel', 'link', 'time', 'embed'])
        failed.reset_index()
        table = pd.read_csv(self.src)

        if not os.path.exists(self.dst):
            os.makedirs(self.dst)

        for i, row in tqdm(table.iterrows(), total=len(table)):
            name = row['name'].lower().replace(' ', '_')
            link = row['link']

            try:
                if 'youtube' in link:
                    vid = YouTube(link)
                    vid.streams.get_highest_resolution().download(self.dst, filename=name)
                elif 'instagram' in link:
                    gram = Instagram(link)
                    gram.download(dest=self.dst, filename=name)
                table.at[i, 'embed'] = name+'.mp4'
            except Exception:
                failed = failed.append(row, ignore_index=True)

        table = table[~table.id.isin(failed.id)]
        table = table.drop(table.columns[0], axis=1)
        failed.to_csv('unavailable.csv')
        table.to_csv('found.csv')
