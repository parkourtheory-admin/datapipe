'''
Data processing pipeline for Parkour Theory

Author: Justin Chen
Date: 5/11/2020
'''

from datacheck import DataCheck
from video import Format
from pprint import pformat
import pandas as pd
import logging as log
import os

def clean_data():
    log.basicConfig(filename=f'errors.log',level=log.DEBUG)

    dc = DataCheck()
    file = 'database/5-10-2020/moves.csv'
    df = pd.read_csv(file, header=0)
    adj = dc.get_adjacency(df)
    err = dc.check_symmetry(adj)
    log.debug(pformat(err))


def format_videos():
    file = 'database/5-10-2020/testclips.csv'
    df = pd.read_csv(file, header=0)
    f = Format(640, 480)

    for i, row in df.iterrows():
        a = row['embed']
        file = f'videos/test/{a}'
        output = row['embed']
        f.resize(file, output)


def main():
    # clean_data()
    format_videos()


if __name__ == '__main__':
    main()
