'''
Author: Justin Chen

main module
'''
import inspect
import argparse
import multiprocessing as mp

import config
from utils import is_config, write, clean_logs, str2bool, accuracy
from tasks import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file in config/')
    parser.add_argument('--clean', '-cl', type=str2bool, help='Clean out old logs')
    args = parser.parse_args()

    if args.clean: clean_logs()

    cfg = config.Configuration(args.config)

    # dynamically import tasks and build pipeline
    pipe = []
    for task in cfg.pipe.split(', '):
        cl = inspect.getmembers(globals()[task], inspect.isclass) # cl is a tuple
        pipe.append(cl[0][1](cfg)) # index 0 is class name and index 1 is object

    log = {}
    if cfg.parallel:
        pipe = [mp.Process(target=t.run) for t in pipe]
        for t in pipe: t.start()
        for t in pipe: t.join()
    else: 
        for i, t in enumerate(pipe):
            task = type(t).__name__
            try: 
                t.run()
                print(f'[{i}] {task} succeeded\n')
            except Exception as e: 
                print(f'[{i}] {task}.py failed\n{str(e)}\n')
                log[type(t).__name__] = str(e)

    accuracy(log, pipe)
    write('datapipe.json', log)


if __name__ == '__main__':
    main()