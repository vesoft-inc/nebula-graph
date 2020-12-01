import sys
from nebula2.common import ttypes
from nebula2.common.ttypes import Value,Vertex,Edge,Path,Step,NullType,DataSet,Row,List
from behave import model as bh
import nbv
from utils import parse


def text2dataset(text):
    lines = text.strip().strip('\n').split('\n')
    head = list()
    temp = lines[0].split('|')
    for item in temp:
        v = str.strip(item).strip('\t')
        if v:
            head.append(v)
    rows = list()
    for i in range(1,len(lines)):
        temp = lines[i].split('|')
        row = list()
        for item in temp:
            v = str.strip(item).strip('\t')
            if len(v) != 0:
                row.append(v)
        if len(row) != 0:
            rows.append(row)
    print(rows)
    table = bh.Table(headings = head, rows = rows)
    dataset = parse(table)
    return dataset



if __name__ == '__main__':
    text='''
          | test   |
            | 2      |
            |33|
    '''
    dataset = text2dataset(text)
    

    
