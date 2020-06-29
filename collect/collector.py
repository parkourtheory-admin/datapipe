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
csv_out     (str) CSV output directory

outputs:
una     (pd.DataFrame) Moves without videos
missing (pd.DataFrame) Moves without videos with a link
cta     (pd.DataFrame) Moves without videos without links
'''
def find_missing(moves_path, videos_path, csv_out):

    moves = pd.read_csv(moves_path, dtype={'id': int})
    clips = pd.read_csv(videos_path,  dtype={'id': int})

    una = clips.loc[clips['embed'] == 'unavailable.mp4']
    una = pd.merge(moves, una, on='id')
    una = una.drop(['prereq', 'subseq', 'type', 'alias', 'desc'], axis=1)
    una.to_csv(os.path.join(csv_out, 'all_missing.csv'), index=False)

    miss = una.loc[una['link'].notnull()]
    miss.to_csv(os.path.join(csv_out, 'missing_with_link.csv'), index=False)

    cta = una.loc[una['link'].isna()]
    cta = cta.drop(['vid', 'channel', 'link', 'time', 'embed'], axis=1)
    cta.to_csv(os.path.join(csv_out, 'call_to_action.csv'), index=False)

    return una, miss, cta


'''
Collect missing videos that have links

inputs:
df      (pd.DataFrame) DataFrame of videos and moves
dst     (str)          Directory to save videos into
csv_out (str)          CSV output directory

outputs:
failed (pd.DataFrame) DataFrame of videos that could not be downloaded. These should be merged with
                      the cta (call to action) dataframe later on.
df     (pd.DataFrame) DataFrame of videos that were able to be downloaded.
'''
def collect(df, dst, csv_out):
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
                gram.download(dst=dst, filename=name)
            df.at[i, 'embed'] = name+'.mp4'
        except Exception:
            failed = failed.append(row, ignore_index=True)

    df = df[~df.id.isin(failed.id)]
    # df = df.drop(df.columns[0], axis=1)
    failed.to_csv('unavailable.csv', index=False)
    df.to_csv('found.csv', index=False)

    return failed, df


'''
Update videos table column and save to csv's

inputs:
video_path (str)          Path to source video csv
update     (pd.DataFrame) DataFrame containing found videos
save_path  (str)          Path including file name for saving csv output
'''
def update_videos(video_path, update, save_path):
    df = pd.read_csv(video_path, dtype={'id': int})
    update.drop(columns='name')

    for i, row in update.iterrows():
        df.at[row['id']-1, 'embed'] = row['embed']

    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.to_csv(save_path, index=False)
