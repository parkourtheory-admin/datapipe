'''
Data processing pipeline for Parkour Theory

Author: Justin Chen
Date: 5/11/2020
'''
import os
import sys
import json
import logging
import argparse
import configparser
from time import time, sleep
from datetime import timedelta, datetime
import multiprocessing as mp
from signal import signal, SIGINT

from pprint import pformat
import pandas as pd

from datacheck import DataCheck
from video import Format
import collector as clt

from more_itertools import chunked
from utils import format_time


'''
Creates subdirectory for logs and logging handler

inputs:
name (str) Log name

outputs:
log (logging.Logger) Log handler
'''
def get_log(name):
    sub = os.path.join('logs', name)

    # check for subdirectory and temporarily change directory permissions to make the directory
    if not os.path.exists(sub):
        try:
            orig = os.umask(0)
            os.makedirs(sub)
        finally:
            os.umask(orig)

    logname = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

    logging.basicConfig(filename=os.path.join(sub, f'{logname}.log'),
                        filemode='w',
                        format='%(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    log = logging.getLogger(name)

    return log


'''
Single execution of pipeline

inputs:
pipe (list) Pipeline specified in config
call (dict) Dictionary containing pointer to function and parameters
'''
def run(pipe, call):
    for f in pipe:
        start = time()
        func = call[f]
        func['name'](*func['params'])
        print(f'{f}()\ttotal time: {format_time(time()-start)}')


'''
Detect ctrl-c

inputs:
sig
frame
'''
def handler(sig, frame):
    sys.exit(0)


'''
Update call map with new parameter

inputs:
calls     (dict)   Map of API calls
new_param (object) New parameter to add to api calls

outputs:
calls (dict) Updated API map
'''
def update_calls(calls, new_param):
    for k, v in calls.items():
        for i, p in enumerate(v['params']):
            if isinstance(new_param, type(p)):
                calls[k]['params'][i] = new_param
    return calls


'''
Continuously check for changes and run pipeline

inputs:
pipe     (list)          Pipeline specified in config
calls    (dict)          Dictionary containing pointer to function and parameters
file     (str)           File to watch
interval (int, optional) Frequency at which pipeline is run
'''
def loop(pipe, calls, file, interval=5):
    if not isinstance(interval, int):
        raise Exception(f'daemonize()\tInvalid parameter interval {interval}. Must be type int.')

    signal(SIGINT, handler)

    while 1:
        df = pd.read_csv(file)
        run(pipe, update_calls(calls, df))
        sleep(interval)


'''
Open and return white list

outputs:
ids (list) List of ids to whitelist
'''
def get_whitelist():
    cfg = configparser.ConfigParser()
    cfg.read(is_config('whitelist'))
    return [int(i) for i in cfg['DEFAULT']['ids'].split(',')]


'''
Data cleaning section of pipeline

inputs:
df        (pd.DataFrame)   Move dataframe
whitelist (list)           Ignore rows corresponding to ids in this list
src       (str)            Source file
log       (logging.Logger) Log file
'''
def check_moves(df, whitelist, src, log=None):
    dc = DataCheck(log, whitelist=whitelist)

    ids = dc.invalid_ids(df)
    print(f'\nINVALID IDS:{ids}\n')

    edges = dc.find_duplicate_edges(df)
    print(f'\nDUP EDGES:{edges}\n')

    dup = dc.duplicated('name', df)
    print(f'\nDUPLICATES:{pformat(dup)}\n')

    adj = dc.get_adjacency(df)
    err = dc.check_symmetry(adj)
    print(f'\nSYMMETRYS: {pformat(err)}\n')

    columns = ['id', 'name', 'type', 'desc']
    print('INCOMPLETE')
    for col in columns:
        print(f'{col}: {dc.find_empty(df, col)}')

    dc.sort_edges(df)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df.to_csv(src, index=False)
    print('EDGES SORTED')


'''
Data collection section of pipeline

inputs:
dst         (str)            Directory where videos will be saved
moves_path  (str)            Path to moves.csv
videos_path (str)            Path to videos.csv
csv_out     (str)            CSV output directory
log         (logging.Logger) Log file
'''
def collect_videos(dst, moves_path, videos_path, csv_out, save_path, log=None):
    una, miss, cta = clt.find_missing(moves_path, videos_path, csv_out)
    
    print(f'una: {len(una)}\tmiss: {len(miss)}\tcta: {len(cta)}')
    miss.to_csv(os.path.join(csv_out, 'missing.csv'))
    una, found = clt.collect(miss, dst, csv_out)

    print(f'una: {len(una)}\tfound: {len(found)}')
    clt.update_videos(videos_path, found, save_path)


'''
Data collection section of pipeline

inputs:
df      (pd.DataFrame)   Table of videos
src_dir (str)            Data directory
dst_dir (str)            Output directory
height  (int)            Output video height
width   (int)            Output video width
log     (logging.Logger) Log file
'''
def format_videos(df, src_dir, dst_dir, height=640, width=480, log=None):
    if not os.path.exists(dst_dir) or len(dst_dir) == 0:
        os.makedirs(dst_dir)

    f = Format(height, width)

    for block in chunked(df.iterrows(), mp.cpu_count()):
        procs = []

        for row in block:
            video = row[1]['embed']
            file = os.path.join(src_dir, video)
            procs.append(mp.Process(target=f.resize, args=(file, os.path.join(dst_dir, video))))

        for p in procs: p.start()
        for p in procs: p.join()


'''
inputs:
cfg  (c)
name (str)

outputs:
calls (dict)
'''
def get_call_map(cfg, name):
    default = cfg['DEFAULT']
    move_pipe = cfg['moves']
    video_pipe = cfg['videos']

    calls = None
    file = ''

    if name == 'moves':
        file = move_pipe['csv']
        df = pd.read_csv(file, header=0)
        whitelist = get_whitelist() if default.getboolean('whitelist') else []
        calls = { 'check_moves': {'name': check_moves, 'params': [df, whitelist, file]} }
    else:
        df = pd.read_csv(video_pipe['csv'], header=0)
        dst = video_pipe['dst']
        file = video_pipe['csv']


        calls = {
            'collect_videos': {'name': collect_videos,
                               'params': [dst, move_pipe['csv'], file, video_pipe['csv_out'], video_pipe['csv']]},
            'format_videos': {'name': format_videos,
                              'params': [df, file, dst]}
        }

    return calls, file

'''
Create pipeline call dictionary and pipeline

inputs:
args      (argparser.ArgumentParser) Pipeline arguments

outputs:
pipe (list) Data pipeline
call (dict) Dictionary of function calls and parameters
'''
def get_pipe(config, name):
    cfg = configparser.ConfigParser()
    cfg.read(config)

    calls, file = get_call_map(cfg, name)
    pipe = cfg[name]['pipe'].split(', ')

    # make log for each API
    for api in pipe:
        params = calls[api]['params']
        params.append(get_log(api))

    return pipe, calls, file


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


'''
Convert argparse arguments into pipeline names

inputs:
p (str) Pipeline argument

outputs:
name (str) Formatted name of pipeline
'''
def is_pipes(p):
    if p == 'm' or p == 'moves' or p == 'move':
        return 'moves'
    elif p == 'v' or p == 'videos' or p == 'video':
        return 'videos'
    else:
        raise Exception('Invalid argparse')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file (available: production, test)')
    parser.add_argument('--loop', '-l', action='store_true', help='Loop execution (default: False)')
    parser.add_argument('--pipes', '-p', type=is_pipes, nargs='+', required=True, help='Specify pipelines to execute. Required by default. (options: m (move), v (video))')
    args = parser.parse_args()

    #TODO: spin off into procs
    for name in args.pipes:
        pipe, calls, file = get_pipe(args.config, name)

        if args.loop: loop(pipe, calls, file)
        else: run(pipe, calls)


if __name__ == '__main__':
    main()
