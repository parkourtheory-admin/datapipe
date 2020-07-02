import os
import time
import json

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
Convert argparse arguments into pipeline names

inputs:
p (str) Pipeline argument

outputs:
name (str) Formatted name of pipeline
'''
def is_pipes(p):
    if p == 'm' or p == 'moves' or p == 'move':
        return 'moves'
    elif p == 'v' or p == 'videos' or p == 'video':
        return 'videos'
    else:
        raise Exception('Invalid argparse')



'''
inputs:
dst (str)
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
log  (dict) Failure log
pipe (list) List of all tasks
'''
def accuracy(log, pipe):
    total = len(pipe)
    failed = len(log)
    completed = total - failed
    
    print(f'completed: {completed} ({completed/total:.2%})')
    print(f'failed:    {failed} ({failed/total:.2%})')
