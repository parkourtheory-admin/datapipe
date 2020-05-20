'''
Data processing pipeline for Parkour Theory

Author: Justin Chen
Date: 5/11/2020
'''
import os
import json
import argparse
import daemon
import configparser
import logging
from time import time, sleep
from datetime import timedelta, datetime
import multiprocessing as mp

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
    fh = logging.FileHandler(os.path.join(sub, f'{logname}.log'))
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    log = logging.getLogger(name)
    log.addHandler(fh)

    return log


'''
Data cleaning section of pipeline

inputs:
df  (pd.DataFrame)   Move dataframe
log (logging.Logger) Log file
'''
def clean_data(df, log=None):
    dc = DataCheck(log)

    ids = dc.invalid_ids(df)
    log.debug(f'Invalid ids:\n{ids}\n')

    dup = dc.duplicated('name', df)
    log.debug(f'Duplicates:\n{pformat(dup)}\n')

    adj = dc.get_adjacency(df)

    err = dc.check_symmetry(adj)
    log.debug(f'\nCheck symmetry() {pformat(err)}\n')

    columns = ['id', 'name', 'type', 'desc']
    for col in columns:
        log.debug(f'incomplete - {col}: {dc.find_empty(df, col)}')


'''
Data collection section of pipeline

inputs:
df  (pd.DataFrame)
log (logging.Logger) Log file
'''
def collect(df, log=None):
    print(log.handlers)
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
    print(log.handlers)
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
Daemonize pipeline

inputs:
pipe     (list)          Pipeline specified in config
call     (dict)          Dictionary containing pointer to function and parameters
daemon   (str, optional) Daemon configuration
interval (int, optional) Frequency at which pipeline is run
'''
def daemonize(pipe, call, daemon='daemon', interval=5):
    if not isinstance(interval, int):
        raise Exception(f'daemonize()\tInvalid parameter interval {interval}. Must be type int.')

    cfg = configparser.ConfigParser()
    cfg.read(daemon)
    cfg = cfg['DEFAULT']

    context = daemon.DaemonContext(
        working_directory=cfg['dir'],
        umask=0o002,
        pidfile=pidfile.TimeoutPIDLockFile(cfg['pid']),
        )

    with context:
        while 1:
            run(pipe, call)
            sleep(interval)


'''
Create pipeline call dictionary and pipeline

inputs:
args (argparser.ArgumentParser) Pipeline arguments

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

    # make log for each API
    for api in pipe:
        calls[api]['params'].append(get_log(api))

    return pipe, calls


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file contain data source and output \directory (available: [production, test])')
    parser.add_argument('--daemon', '-d', action='store_true', help='Daemonize repeated execution (default: False)')
    args = parser.parse_args()

    pipe, call = get_pipe(args)

    if args.daemon: daemonize(pipe, call)
    else: run(pipe, call)


if __name__ == '__main__':
    main()
