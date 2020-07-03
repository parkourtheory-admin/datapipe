import os
import time
import json
import functools

def format_time(t):
    h, r = divmod(t, 3600)
    m, s = divmod(r, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(h), int(m), s)


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
inputs:
dst  (str)  Log file name
data (dict) Log data
'''
def write(dst, data):
    with open(os.path.join('logs', dst), 'w') as file:
        json.dump(data, file)


'''
Remove all logs
'''
def clean_logs():
    for f in os.listdir('./logs'):
        os.remove(os.path.join('./logs',f))
        

'''
Display pipeline completion accuracy
inputs:
log   (dict)     Failure log
total (iterable) Iterable object of all tasks
'''
def accuracy(log, total):
    total = len(total)
    failed = len(log)
    completed = total - failed
    
    print(f'completed: {completed} ({completed/total:.2%})')
    print(f'failed:    {failed} ({failed/total:.2%})')


'''
timer decorator

inputs:
func (function) Function to time

outputs:
func (function) Wrapper for functio to be timed
'''
def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start = time.perf_counter()
        value = func(*args, **kwargs)
        runtime = time.perf_counter() - start
        print(f'{func.__name__}() time: {runtime:.4f} s')
        return value
    return wrapper_timer