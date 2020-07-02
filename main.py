'''
Author: Justin Chen

main module
'''
import inspect
import argparse
import traceback
import multiprocessing as mp

import config
from utils import is_config, write, clean_logs, accuracy
from tasks import *

'''
Parallel task execution

inputs:
pipe (list) Tasks to execute
'''
def parallel(pipe):
    pipe = [mp.Process(target=t.run) for t in pipe]
    for t in pipe: t.start()
    for t in pipe: t.join()

'''
Sequential execution of pipeline

inputs:
pipe (list) Tasks to execute
log  (dict) Log for failed tasks
'''
def sequential(pipe, log, verbose=True):
    for i, t in enumerate(pipe):
        task = type(t).__name__
        try: 
            t.run()
            print(f'[{i}] {task} succeeded\n')
        except Exception as e:
            tb = traceback.format_exc()
            err = tb if verbose else str(e)
            
            print(f'[{i}] {task}.py failed\n\n{err}\n')

            log[type(t).__name__] = tb


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file in config/')
    parser.add_argument('--clean', '-cl', action='store_true', help='Clean out old logs')
    parser.add_argument('--verbose', '-vb', action='store_true', help='Display stack trace if errors occur')
    args = parser.parse_args()

    if args.clean: clean_logs()

    cfg = config.Configuration(args.config)

    # dynamically import tasks and build pipeline
    pipe = []
    for task in cfg.pipe.split(', '):
        cl = inspect.getmembers(globals()[task], inspect.isclass) # cl is a tuple
        pipe.append(cl[0][1](cfg)) # index 0 is class name and index 1 is object

    log = {}
    if cfg.parallel: parallel(pipe)
    else: sequential(pipe, log, verbose=args.verbose)

    accuracy(log, pipe)
    write('datapipe.json', log)


if __name__ == '__main__':
    main()