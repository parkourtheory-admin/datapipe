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
from collector import Collector

from more_itertools import chunked


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
Data cleaning section of pipeline

inputs:
df        (pd.DataFrame)   Move dataframe
log       (logging.Logger) Log file
whitelist (list)           Ignore rows corresponding to ids in this list
'''
def clean_data(df, log=None, whitelist=None):
    dc = DataCheck(log, whitelist=whitelist)

    ids = dc.invalid_ids(df)
    print(f'\nINVALID IDS:{ids}\n')

    dup = dc.duplicated('name', df)
    print(f'\nDUPLICATES:{pformat(dup)}\n')

    adj = dc.get_adjacency(df)
    err = dc.check_symmetry(adj)
    print(f'\nSYMMETRYS: {pformat(err)}\n')

    columns = ['id', 'name', 'type', 'desc']
    print('INCOMPLETE')
    for col in columns:
        print(f'{col}: {dc.find_empty(df, col)}')


'''
Data collection section of pipeline

inputs:
df  (pd.DataFrame)
log (logging.Logger) Log file
'''
def collect(df, log=None):
    col = Collector()
    col.collect()


'''
Data collection section of pipeline

inputs:
df       (pd.DataFrame)   Table of moves
data_dir (str)            Data directory
out_dir  (str)            Output directory
log      (logging.Logger) Log file
'''
def format_videos(df, data_dir, out_dir='', log=None):
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
Single execution of pipeline

inputs:
pipe (list) Pipeline specified in config
call (dict) Dictionary containing pointer to function and parameters
'''
def run(pipe, call):
    for f in pipe:
        func = call[f]
        func['name'](*func['params'])


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
Create pipeline call dictionary and pipeline

inputs:
args      (argparser.ArgumentParser) Pipeline arguments

outputs:
pipe (list) Data pipeline
call (dict) Dictionary of function calls and parameters
'''
def get_pipe(args):
    cfg = configparser.ConfigParser()
    cfg.read(args.config)
    cfg = cfg['DEFAULT']
    df = pd.read_csv(cfg['csv'], header=0)
    out_dir = cfg['output_dir']
    data_dir = cfg['data_dir']

    calls =  {
        'collect': {'name': collect, 'params': [df]},
        'clean_data': {'name': clean_data, 'params': [df]},
        'format_videos': {'name': format_videos, 'params': [df, data_dir, out_dir]}
    }

    pipe = cfg['pipe'].split()

    whitelist = get_whitelist() if cfg['whitelist'] else None

    # make log for each API
    for api in pipe:
        params = calls[api]['params']
        params.append(get_log(api))

        if whitelist:
            params.append(whitelist)

    return pipe, calls, cfg['csv']


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
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file contain data source and output directory')
    parser.add_argument('--loop', '-l', action='store_true', help='Loop execution (default: False)')
    args = parser.parse_args()
    pipe, calls, file = get_pipe(args)

    if args.loop: loop(pipe, calls, file)
    else: run(pipe, calls)


if __name__ == '__main__':
    main()
