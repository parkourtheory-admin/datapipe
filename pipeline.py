'''
Data pipeline API
'''
import os
import time
import traceback
import multiprocessing as mp

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
            print(f'{Fore.GREEN}[{i}] {task} succeeded - time: {runtime:.4f} s\n{Style.RESET_ALL}')
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
	p = set(pipe)
	assert len(p) == len(pipe)
	return p


'''
Check that all specified tasks exists in tasks/

inputs:
pipe (list) List of tasks
'''
def exists(pipe):
	tasks = [t.split('.')[0] for t in os.listdir('tasks') if t.endswith('.py')]
	for t in pipe:
		assert t in tasks
