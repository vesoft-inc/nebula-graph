# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.
#

import ply.lex as lex
import ply.yacc as yacc

from nebula2.common.ttypes import Value,List,NullType,Map,List,Set,Vertex,Tag,Edge,Path,Step
Value.__hash__ = lambda self: self.value.__hash__()


states = (
    ('sstr', 'exclusive'),
    ('dstr', 'exclusive'),
)

tokens = (
    'EMPTY',
    'NULL',
    'NaN',
    'BAD_DATA',
    'BAD_TYPE',
    'OVERFLOW',
    'UNKNOWN_PROP',
    'DIV_BY_ZERO',
    'OUT_OF_RANGE',
    'FLOAT',
    'INT',
    'STRING',
    'BOOLEAN',
    'LABEL',
)

literals = ['(', ')', '[', ']', '{', '}', '<', '>', '@', '-', ':', ',']

t_LABEL = r'[_a-zA-Z][_a-zA-Z0-9]*'

t_ignore = ' \t\n'
t_sstr_dstr_ignore = ''

def t_EMPTY(t):
    r'EMPTY'
    t.value = Value()
    return t

def t_NULL(t):
    r'NULL'
    t.value = Value(nVal = NullType.__NULL__)
    return t

def t_NaN(t):
    r'NaN'
    t.value = Value(nVal = NullType.NaN)
    return t

def t_BAD_DATA(t):
    r'BAD_DATA'
    t.value = Value(nVal = NullType.BAD_DATA)
    return t

def t_BAD_TYPE(t):
    r'BAD_TYPE'
    t.value = Value(nVal = NullType.BAD_TYPE)
    return t

def t_OVERFLOW(t):
    r'OVERFLOW'
    t.value = Value(nVal = NullType.ERR_OVERFLOW)
    return t

def t_UNKNOWN_PROP(t):
    r'UNKNOWN_PROP'
    t.value = Value(nVal = NullType.UNKNOWN_PROP)
    return t

def t_DIV_BY_ZERO(t):
    r'DIV_BY_ZERO'
    t.value = Value(nVal = NullType.DIV_BY_ZERO)
    return t

def t_OUT_OF_RANGE(t):
    r'OUT_OF_RANGE'
    t.value = Value(nVal = NullType.OUT_OF_RANGE)
    return t

def t_FLOAT(t):
    r'-?\d+\.\d+'
    t.value = Value(fVal = float(t.value))
    return t

def t_INT(t):
    r'-?\d+'
    t.value = Value(iVal = int(t.value))
    return t

def t_BOOLEAN(t):
    r'(?i)true|false'
    v = Value()
    if t.value.lower() == 'true':
        v.set_bVal(True)
    else:
        v.set_bVal(False)
    t.value = v
    return t

def t_sstr(t):
    r'\''
    t.lexer.string = ''
    t.lexer.begin('sstr')
    pass

def t_dstr(t):
    r'"'
    t.lexer.string = ''
    t.lexer.begin('dstr')
    pass

def t_sstr_dstr_escape_newline(t):
    r'\\n'
    t.lexer.string += '\n'
    pass

def t_sstr_dstr_escape_tab(t):
    r'\\t'
    t.lexer.string += '\t'
    pass

def t_sstr_dstr_escape_char(t):
    r'\\.'
    t.lexer.string += t.value[1]
    pass

def t_sstr_any(t):
    r'[^\']'
    t.lexer.string += t.value
    pass

def t_dstr_any(t):
    r'[^"]'
    t.lexer.string += t.value
    pass

def t_sstr_STRING(t):
    r'\''
    t.value = Value(sVal = t.lexer.string)
    t.lexer.begin('INITIAL')
    return t

def t_dstr_STRING(t):
    r'"'
    t.value = Value(sVal = t.lexer.string)
    t.lexer.begin('INITIAL')
    return t

def t_ANY_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)

def p_expr(p):
    '''
        expr : EMPTY
             | NULL
             | NaN
             | BAD_DATA
             | BAD_TYPE
             | OVERFLOW
             | UNKNOWN_PROP
             | DIV_BY_ZERO
             | OUT_OF_RANGE
             | INT
             | FLOAT
             | BOOLEAN
             | STRING
             | list
             | set
             | map
             | vertex
             | edge
             | path
             | function
    '''
    p[0] = p[1]

def p_list(p):
    '''
        list : '[' list_items ']'
             | '[' ']'
    '''
    l = NList()
    if len(p) == 4:
        l.values = p[2]
    else:
        l.values = []
    p[0] = Value(lVal = l)

def p_set(p):
    '''
        set : '{' list_items '}'
    '''
    s = NSet()
    s.values = set(p[2])
    p[0] = Value(uVal = s)

def p_list_items(p):
    '''
        list_items : expr
                   | list_items ',' expr
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[3])
        p[0] = p[1]

def p_map(p):
    '''
        map : '{' map_items '}'
            | '{' '}'
    '''
    m = NMap()
    if len(p) == 4:
        m.kvs = p[2]
    else:
        m.kvs = {}
    p[0] = Value(mVal = m)

def p_map_items(p):
    '''
        map_items : LABEL ':' expr
                  | STRING ':' expr
                  | map_items ',' LABEL ':' expr
                  | map_items ',' STRING ':' expr
    '''
    if len(p) == 4:
        k = p[1]
        if isinstance(k, Value):
            k = k.get_sVal()
        p[0] = {k: p[3]}
    else:
        k = p[3]
        if isinstance(k, Value):
            k = k.get_sVal()
        p[1][k] = p[5]
        p[0] = p[1]

def p_vertex(p):
    '''
        vertex : '(' tag_list ')'
               | '(' STRING tag_list ')'
    '''
    vid = None
    tags = None
    if len(p) == 4:
        tags = p[2]
    else:
        vid = p[2].get_sVal()
        tags = p[3]
    v = Vertex(vid = vid, tags = tags)
    p[0] = Value(vVal = v)

def p_tag_list(p):
    '''
        tag_list :
                 | tag_list tag
    '''
    if len(p) == 3:
        if p[1] is None:
            p[1] = []
        p[1].append(p[2])
        p[0] = p[1]

def p_tag(p):
    '''
        tag : ':' LABEL map
            | ':' LABEL
    '''
    tag = Tag(name = p[2])
    if len(p) == 4:
        tag.props = p[3].get_mVal().kvs
    p[0] = tag

def p_edge(p):
    '''
        edge : '-' edge_spec '-' '>'
             | '<' '-' edge_spec '-'
    '''
    if p[1] == '-':
        e = p[2]
        e.type = 1
    else:
        e = p[3]
        e.type = -1
    p[0] = Value(eVal = e)

def p_edge_spec(p):
    '''
        edge_spec :
                  | '[' edge_rank edge_props ']'
                  | '[' ':' LABEL edge_rank edge_props ']'
                  | '[' STRING '-' '>' STRING edge_rank edge_props ']'
                  | '[' ':' LABEL STRING '-' '>' STRING edge_rank edge_props ']'
    '''
    e = Edge()
    if len(p) == 5:
        e.ranking = p[2]
        e.props = p[3]
    elif len(p) == 7:
        e.name = p[3]
        e.ranking = p[4]
        e.props = p[5]
    elif len(p) == 9:
        e.src = p[2].get_sVal()
        e.dst = p[5].get_sVal()
        e.ranking = p[6]
        e.props = p[7]
    elif len(p) == 11:
        e.name = p[3]
        e.src = p[4].get_sVal()
        e.dst = p[7].get_sVal()
        e.ranking = p[8]
        e.props = p[9]
    p[0] = e

def p_edge_rank(p):
    '''
        edge_rank :
                  | '@' INT
    '''
    if len(p) == 1:
        p[0] = None
    else:
        p[0] = p[2].get_iVal()

def p_edge_props(p):
    '''
        edge_props :
                   | map
    '''
    if len(p) == 1:
        p[0] = None
    else:
        p[0] = p[1].get_mVal().kvs

def p_path(p):
    '''
        path : '<' vertex steps '>'
             | '<' vertex '>'
    '''
    path = Path()
    path.src = p[2].get_vVal()
    if len(p) == 5:
        path.steps = p[3]
    p[0] = Value(pVal = path)

def p_steps(p):
    '''
        steps : edge vertex
              | steps edge vertex
    '''
    step = Step()
    if len(p) == 3:
        step.dst = p[2].get_vVal()
        edge = p[1].get_eVal()
        step.name = edge.name
        step.type = edge.type
        step.ranking = edge.ranking
        step.props = edge.props
        p[0] = [step]
    else:
        step.dst = p[3].get_vVal()
        edge = p[2].get_eVal()
        step.name = edge.name
        step.type = edge.type
        step.ranking = edge.ranking
        step.props = edge.props
        p[1].append(step)
        p[0] = p[1]

def p_function(p):
    '''
        function : LABEL '(' list_items ')'
    '''
    p[0] = functions[p[1]](*p[3])

def p_error(p):
    print("Syntax error in input!")

lexer = lex.lex()
parser = yacc.yacc()
functions = {}

def register_function(name, func):
    functions[name] = func

def parse(s):
    return parser.parse(s)

def parse_row(row):
    return [str(parse(x)) for x in row]


if __name__ == '__main__':
    expected = {}
    expected['EMPTY'] = Value()
    expected['NULL'] = Value(nVal = NullType.__NULL__)
    expected['NaN'] = Value(nVal = NullType.NaN)
    expected['BAD_DATA'] = Value(nVal = NullType.BAD_DATA)
    expected['BAD_TYPE'] = Value(nVal = NullType.BAD_TYPE)
    expected['OVERFLOW'] = Value(nVal = NullType.ERR_OVERFLOW)
    expected['UNKNOWN_PROP'] = Value(nVal = NullType.UNKNOWN_PROP)
    expected['DIV_BY_ZERO'] = Value(nVal = NullType.DIV_BY_ZERO)
    expected['OUT_OF_RANGE'] = Value(nVal = NullType.OUT_OF_RANGE)
    expected['123'] = Value(iVal = 123)
    expected['-123'] = Value(iVal = -123)
    expected['3.14'] = Value(fVal = 3.14)
    expected['-3.14'] = Value(fVal = -3.14)
    expected['true'] = Value(bVal = True)
    expected['True'] = Value(bVal = True)
    expected['false'] = Value(bVal = False)
    expected['fAlse'] = Value(bVal = False)
    expected["'string'"] = Value(sVal = "string")
    expected['"string"'] = Value(sVal = 'string')
    expected['''"string'string'"'''] = Value(sVal = "string'string'")
    expected['[]'] = Value(lVal = List([]))
    expected['[1,2,3]'] = Value(lVal = List([Value(iVal=1),Value(iVal=2),Value(iVal=3)]))
    expected['{1,2,3}'] = Value(uVal = Set(set([Value(iVal=1),Value(iVal=2),Value(iVal=3)])))
    expected['{}'] = Value(mVal = Map({}))
    expected['{k1:1,"k2":true}'] = Value(mVal = Map({'k1': Value(iVal=1), 'k2': Value(bVal=True)}))
    expected['()'] = Value(vVal = Vertex())
    expected['("vid")'] = Value(vVal = Vertex(vid = 'vid'))
    expected['("vid":t)'] = Value(vVal=Vertex(vid='vid',tags=[Tag(name='t')]))
    expected['("vid":t:t)'] = Value(vVal=Vertex(vid='vid',tags=[Tag(name='t'),Tag(name='t')]))
    expected['("vid":t{p1:0,p2:" "})'] = Value(vVal=Vertex(vid='vid',\
                tags=[Tag(name='t',props={'p1':Value(iVal=0),'p2':Value(sVal=' ')})]))
    expected['("vid":t1{p1:0,p2:" "}:t2{})'] = Value(vVal=Vertex(vid='vid',\
                tags=[Tag(name='t1',props={'p1':Value(iVal=0),'p2':Value(sVal=' ')}),\
                        Tag(name='t2',props={})]))
    expected['-->'] = Value(eVal=Edge(type=1))
    expected['<--'] = Value(eVal=Edge(type=-1))
    expected['-[]->'] = Value(eVal=Edge(type=1))
    expected['<-[]-'] = Value(eVal=Edge(type=-1))
    expected['-[:e]->'] = Value(eVal=Edge(name='e',type=1))
    expected['<-[:e]-'] = Value(eVal=Edge(name='e',type=-1))
    expected['-[@1]->'] = Value(eVal=Edge(type=1,ranking=1))
    expected['-[@-1]->'] = Value(eVal=Edge(type=1,ranking=-1))
    expected['<-[@-1]-'] = Value(eVal=Edge(type=-1,ranking=-1))
    expected['-["1"->"2"]->'] = Value(eVal=Edge(src='1',dst='2',type=1))
    expected['<-["1"->"2"]-'] = Value(eVal=Edge(src='1',dst='2',type=-1))
    expected['-[{}]->'] = Value(eVal=Edge(type=1,props={}))
    expected['<-[{}]-'] = Value(eVal=Edge(type=-1,props={}))
    expected['-[:e{}]->'] = Value(eVal=Edge(name='e',type=1,props={}))
    expected['<-[:e{}]-'] = Value(eVal=Edge(name='e',type=-1,props={}))
    expected['-[:e@123{}]->'] = Value(eVal=Edge(name='e',type=1,ranking=123,props={}))
    expected['<-[:e@123{}]-'] = Value(eVal=Edge(name='e',type=-1,ranking=123,props={}))
    expected['-[:e"1"->"2"@123{}]->'] = Value(eVal=Edge(name='e',type=1,ranking=123,src='1',dst='2',props={}))
    expected['<()>'] = Value(pVal=Path(src=Vertex()))
    expected['<("vid")>'] = Value(pVal=Path(src=Vertex(vid='vid')))
    expected['<()-->()>'] = Value(pVal=Path(src=Vertex(),steps=[Step(type=1, dst=Vertex())]))
    expected['<()<--()>'] = Value(pVal=Path(src=Vertex(),steps=[Step(type=-1, dst=Vertex())]))
    expected['<()-->()-->()>'] = Value(pVal=Path(src=Vertex(),\
                steps=[Step(type=1, dst=Vertex()),Step(type=1, dst=Vertex())]))
    expected['<()-->()<--()>'] = Value(pVal=Path(src=Vertex(),\
                steps=[Step(type=1, dst=Vertex()),Step(type=-1, dst=Vertex())]))
    expected['<("v1")-[:e1]->()<-[:e2]-("v2")>'] = Value(pVal=Path(src=Vertex(vid='v1'),\
                steps=[Step(name='e1',type=1,dst=Vertex()),Step(name='e2',type=-1, dst=Vertex(vid='v2'))]))
    for item in expected.items():
        v = parse(item[0])
        assert v is not None, "Failed to parse %s" % item[0]
        assert v == item[1], \
                  "Parsed value not as expected, str: %s, expected: %s actual: %s" % (item[0], item[1], v)
