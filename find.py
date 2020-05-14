import pandas as pd
import re
import pprint as pp

# file = '../data/database/5-10-2020/moves.csv'
# df = pd.read_csv(file, header=0)
# print(df.loc[df['moveName'] == 'Palm Gainer'])

done = set()
todo = []
with open('broken.txt', 'r') as broken:
    for row in broken.readlines():
        row = row.split('  [')
        edge = tuple(row[-1].replace(']','').rstrip().split())
        todo.append(edge) if row[0] == '- [ ]' else done.add(edge)

t = []
for i in todo:
    if i[::-1] not in t:
        t.append(i)
todo = ['- [ ]  '+' '.join(i) for i in t if i[::-1] not in done]
print(*todo, sep='\n')
print(len(todo))
