'''
Module for dynamically importing <task>.py files when the tasks module is imported
Importing class must import all
i.e. from tasks import *

Instructions for creating a new task:
1. All <task>.py files should be named using camelcase and with the same name as the class
   e.g. CheckMoves.py, which contains 
        class CheckMoves(object):
2. Each task should take a Configuration object via constructor
3. Each task should have a run()
'''
from os import listdir
from os.path import dirname, basename

__all__ = [basename(f)[:-3] for f in listdir(dirname(__file__)) if f[-3:] == ".py" and not f.endswith("__init__.py")]