'''
Data pipeline API
'''
import os
import sys
import time
import inspect
import traceback
import multiprocessing as mp
from collections import OrderedDict

from tasks import *

from colorama import Fore, Style
from utils import timer

'''
Parallel task execution

inputs:
pipe (list) Tasks to execute
'''
@timer
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
@timer
def sequential(pipe, log, verbose=True):
    for i, t in enumerate(pipe):
        task = type(t).__name__

        try: 
            start = time.perf_counter()
            t.run()
            runtime = time.perf_counter() - start
            print(f'{Fore.GREEN}[{i}] {task} succeeded - time: {runtime:.4f} s{Style.RESET_ALL}')
        except Exception as e:
            tb = traceback.format_exc()
            err = tb if verbose else str(e)
            
            print(f'{Fore.RED}[{i}] {task}.py failed{Style.RESET_ALL}')
            print(f'{err}\n')

            log[type(t).__name__] = tb


'''
Check that pipe is unique

inputs:
pipe (list) List of tasks

output:
pipe (set) Unique set of tasks
'''
def unique(pipe):
	p = list(OrderedDict.fromkeys(pipe).keys())
	assert len(p) == len(pipe)
	return p


'''
Check that all specified tasks exists in tasks/

inputs:
pipe (list) List of tasks
'''
def exists(pipe):
    tasks = [t.split('.')[0] for t in os.listdir('tasks') if t.endswith('.py')]
    errs = []
    for t in pipe:
        if t not in tasks: errs.append(t)

    if len(errs) > 0: 
        print(f'Invalid tasks: {Fore.RED}{", ".join(errs)}{Style.RESET_ALL}')
        sys.exit(0)


def notice():
    print(f'{Fore.BLUE}REMINDER:{Style.RESET_ALL} check logs/ for output even if tasks are successful.')


'''
Prompt user if they would like to continue. Guarda against running too many tasks accidentally.

inputs:
cfg (config.Configuration) Configuration instance of config file
'''
def prompt(cfg, tasks):
    if cfg.warning:
        resp = input(f'{Fore.GREEN}{len(tasks)} tasks in pipeline{Style.RESET_ALL}\nProceed ([y]/n)? ')
        if resp.lower() in ('ie', 'no', 'n'):
            sys.exit(0)


'''
Dynamically import tasks and build pipeline

inputs:
cfg (config.Configuration) Configuration instance of config file

outputs:
pipe (list) List of task objects
'''
def build(cfg):
    tasks = unique(cfg.pipe.split(', '))
    prompt(cfg, tasks)
    exists(tasks)

    pipe = []

    for t in tasks:
        cl = inspect.getmembers(globals()[t], inspect.isclass) # cl is a tuple
        pipe.append(cl[0][1](cfg)) # index 0 is class name and index 1 is object

    return pipe