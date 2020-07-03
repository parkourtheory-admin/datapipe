'''
Author: Justin Chen

main module
'''
import inspect
import argparse
import config

from tasks import *
from pipeline import parallel, sequential, unique, exists, notice
from utils import is_config, write, clean_logs, accuracy, timer


@timer
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file in config/')
    parser.add_argument('--clean', '-cl', action='store_true', help='Clean out old logs')
    parser.add_argument('--verbose', '-v', action='store_true', help='Display stack trace if errors occur')
    args = parser.parse_args()

    if args.clean: clean_logs()

    cfg = config.Configuration(args.config)

    # dynamically import tasks and build pipeline
    pipe = []
    tasks = unique(cfg.pipe.split(', '))
    exists(tasks)

    for task in tasks:
        cl = inspect.getmembers(globals()[task], inspect.isclass) # cl is a tuple
        pipe.append(cl[0][1](cfg)) # index 0 is class name and index 1 is object

    log = {}
    if cfg.parallel: parallel(pipe)
    else: sequential(pipe, log, verbose=args.verbose)

    accuracy(log, pipe)
    write('datapipe.json', log)
    notice()


if __name__ == '__main__':
    main()