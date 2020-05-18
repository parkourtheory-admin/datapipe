'''
Interface for finding moves
'''

import re
import argparse
import pandas as pd
import pprint as pp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--move', '-m', type=str)
    args = parser.parse_args()

    file = '../data/database/5-17-2020/moves.csv'
    df = pd.read_csv(file, header=0)
    print(df.loc[df['name'] == args.move.title()])


if __name__ == '__main__':
    main()



# done = set()
# todo = []
# with open('broken.txt', 'r') as broken:
#     for row in broken.readlines():
#         row = row.split('  [')
#         edge = tuple(row[-1].replace(']','').rstrip().split())
#         todo.append(edge) if row[0] == '- [ ]' else done.add(edge)
#
# t = []
# for i in todo:
#     if i[::-1] not in t:
#         t.append(i)
# todo = ['- [ ]  '+' '.join(i) for i in t if i[::-1] not in done]
# print(*todo, sep='\n')
# print(len(todo))
