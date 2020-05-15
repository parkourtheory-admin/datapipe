'''
Data processing pipeline for Parkour Theory

Author: Justin Chen
Date: 5/11/2020
'''
import os
import json
import argparse
import configparser
import logging as log
from time import time
from datetime import timedelta
import multiprocessing as mp

from pprint import pformat
import pandas as pd

from datacheck import DataCheck
from video import Format
from collector import Collector

from more_itertools import chunked

'''
Data collection section of pipeline

inputs:
df (pd.DataFrame)
'''
def collect(df):
    col = Collector()
    col.collect()


'''
Data cleaning section of pipeline

inputs:
df (pd.DataFrame)
'''
def clean_data(df):
    log.basicConfig(filename='errors.log',level=log.DEBUG)

    dc = DataCheck()
    adj = dc.get_adjacency(df)
    err = dc.check_symmetry(adj)
    log.debug(pformat(err))

    dup = dc.duplicated(df)
    log.debug(pformat(dup))

    ids = dc.valid_ids(df)
    log.debug(f'consistent ids: {ids}')


'''
Data collection section of pipeline

inputs:
df (pd.DataFrame)
data_dir (str)
out_dir (str)
'''
def format_videos(df, data_dir, out_dir=''):
    if not os.path.exists(out_dir) or len(out_dir) == 0:
        os.makedirs(out_dir)

    f = Format(640, 480)

    cores = mp.cpu_count()
    for block in chunked(df.iterrows(), cores):
        procs = []

        for row in block:
            video = row[1]['embed']
            file = os.path.join(data_dir, video)
            procs.append(mp.Process(target=f.resize, args=(file, os.path.join(out_dir, video))))

        for p in procs: p.start()
        for p in procs: p.join()


'''
Format argparse configuration file parameter

inputs:
c (str) Configuration file name

outputs:
(str) formatted file name
'''
def is_config(c):
    if not c.endswith('.ini'):
        c = c.split('.')[0]+'.ini'
    return os.path.join('configs', c)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file contain data source and output \directory (available: [production, test])')
    args = parser.parse_args()

    if not os.path.exists('log'):
        os.makedirs('log')

    cfg = configparser.ConfigParser()
    cfg.read(args.config)
    cfg = cfg['DEFAULT']
    df = pd.read_csv(cfg['csv'], header=0)
    out_dir = cfg['output_dir']
    data_dir = cfg['data_dir']

    call = {
        'collect': {'name': collect, 'params': [df]},
        'clean_data': {'name': clean_data, 'params': [df]},
        'format_videos': {'name': format_videos, 'params': [df, data_dir, out_dir]}
    }

    for f in cfg['pipe'].split():
        func = call[f]
        func['name'](*func['params'])


if __name__ == '__main__':
    main()
