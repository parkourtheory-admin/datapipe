import os
import sys
import pandas as pd
from tqdm import tqdm
from pytube import YouTube

sys.path.insert(1, '/media/ch3njus/Seagate4TB/projects/pystagram')

from pystagram import Instagram


'''
Output csvs of missing moves without videos

inputs:
moves_path  (str) Path to moves.csv
videos_path (str) Path to videos.csv

outputs:
una     (pd.DataFrame) Moves without videos
missing (pd.DataFrame) Moves without videos with a link
cta     (pd.DataFrame) Moves without videos without links
'''
def find_missing(moves_path, videos_path):

    moves = pd.read_csv(moves_path, dtype={'id': int, 'rank': int})
    clips = pd.read_csv(videos_path,  dtype={'id': int})

    una = clips.loc[clips['embed'] == 'unavailable.mp4']
    una = pd.merge(moves, una, on='id')
    una = una.drop(['prereq', 'nextmove', 'moveType', 'alias', 'descript', 'rank'], axis=1)
    una.to_csv('all_missing.csv')

    miss = una.loc[una['link'].notnull()]
    miss.to_csv('missing_with_link.csv')

    cta = una.loc[una['link'].isna()]
    cta = cta.drop(['vid', 'channel', 'link', 'time', 'embed'], axis=1)
    cta.to_csv('call_to_action.csv')

    return una, miss, cta


'''
Collect missing videos that have links

inputs:
df  (pd.DataFrame) DataFrame of videos and moves
dst (str)          Directory to save videos into

outputs:
failed (pd.DataFrame) DataFrame of videos that could not be downloaded. These should be merged with
                      the cta (call to action) dataframe later on.
df     (pd.DataFrame) DataFrame of videos that were able to be downloaded.
'''
def collect(df, dst):
    failed = pd.DataFrame(columns=['id', 'name', 'vid', 'channel', 'link', 'time', 'embed'])
    failed.reset_index()

    if not os.path.exists(dst):
        os.makedirs(dst)

    for i, row in tqdm(df.iterrows(), total=len(df)):
        # format video file name
        name = row['name'].lower().replace(' ', '_')
        link = row['link']

        try:
            if 'youtube' in link:
                vid = YouTube(link)
                vid.streams.get_highest_resolution().download(dst, filename=name)
            elif 'instagram' in link:
                gram = Instagram(link)
                gram.download(dest=dst, filename=name)
            df.at[i, 'embed'] = name+'.mp4'
        except Exception:
            failed = failed.append(row, ignore_index=True)

    df = df[~df.id.isin(failed.id)]
    df = df.drop(df.columns[0], axis=1)
    failed.to_csv('unavailable.csv')
    df.to_csv('found.csv')

    return failed, df
