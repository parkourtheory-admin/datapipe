'''
Author: Justin Chen

main module
'''
import argparse
import config

from pipeline import parallel, sequential, build, notice
from utils import *


@timer
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', '-cfg', type=is_config, help='Configuration file in config/')
    parser.add_argument('--clean', '-c', action='store_true', help='Clean out old logs')
    parser.add_argument('--verbose', '-v', action='store_true', help='Display stack trace if errors occur')
    args = parser.parse_args()

    make_dir('logs')
    if args.clean: clean_logs()

    cfg = config.Configuration(args.config)
    pipe = build(cfg)

    log = {}
    if cfg.parallel: parallel(pipe)
    else: sequential(pipe, log, verbose=args.verbose)

    accuracy(log, pipe)
    write('datapipe.json', log)
    notice()


if __name__ == '__main__':
    main()