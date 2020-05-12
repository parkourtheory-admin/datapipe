'''
Data processing pipeline for Parkour Theory

Author: Justin Chen
Date: 5/11/2020
'''
import os
import logging as log
from time import time
from datetime import timedelta
import multiprocessing as mp
from more_itertools import chunked
from pprint import pformat
import pandas as pd
from datacheck import DataCheck
from video import Format


from more_itertools import chunked

def clean_data():
    log.basicConfig(filename=f'errors.log',level=log.DEBUG)

    dc = DataCheck()
    file = 'database/5-10-2020/moves.csv'
    df = pd.read_csv(file, header=0)
    adj = dc.get_adjacency(df)
    err = dc.check_symmetry(adj)
    log.debug(pformat(err))


def format_videos(out_dir=''):
    if not os.path.exists(out_dir) or len(out_dir) == 0:
        os.makedirs(out_dir)

    file = '../data/database/5-10-2020/testclips.csv'
    df = pd.read_csv(file, header=0)
    f = Format(640, 480)

    cores = mp.cpu_count()
    for block in chunked(df.iterrows(), cores):
        procs = []

        for row in block:
            video = row[1]['embed']
            file = f'../data/videos/test/{video}'
            procs.append(mp.Process(target=f.resize, args=(file, os.path.join(out_dir, video))))

        for p in procs: p.start()
        for p in procs: p.join()


def main():
    clean_data()
    out_dir = '/media/ch3njus/Seagate4TB/research/parkourtheory/datapipe/output'
    format_videos(out_dir)


if __name__ == '__main__':
    main()
