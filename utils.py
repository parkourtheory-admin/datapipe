import time

def format_time(t):
    h, r = divmod(t, 3600)
    m, s = divmod(r, 60)
    return "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)
