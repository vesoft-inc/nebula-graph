import sys
sys.path.insert(0,'../nebula-clients/python/')
from nebula2.gclient.data.DataObject import DataSetWrapper
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
        value = list()
        for item in temp:
            v = str.strip(item).strip('\t')
            if v:
                value.append(v)
        if value:
            rows.append(value)
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
    

    
