'''
Author: Justin Chen

main module
'''
import inspect
import argparse
import multiprocessing as mp

import config
from utils import is_config
from tasks import *


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file (available: production, test)')
    args = parser.parse_args()

    cfg = config.Configuration(args.config)

    # dynamically import tasks and build pipeline
    pipe = []
    for task in cfg.pipe.split(', '):
        cl = inspect.getmembers(globals()[task], inspect.isclass)
        pipe.append(cl[0][1](cfg))

    if cfg.parallel:
        pipe = [mp.Process(target=t.run) for t in pipe]
        for t in pipe: t.start()
        for t in pipe: t.join()
    else: 
        for t in pipe: t.run()


if __name__ == '__main__':
    main()