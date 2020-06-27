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


def write(dst, data):
    with open(dst, 'w') as file:
        json.dump(data, file)