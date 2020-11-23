import ply.lex as lex
import ply.yacc as yacc
from nebula2.common import ttypes as nb
nb.Value.__hash__ = lambda self: self.value.__hash__()


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
    t.value = nb.Value()
    return t

def t_NULL(t):
    r'NULL'
    t.value = nb.Value(nVal = nb.NullType.__NULL__)
    return t

def t_NaN(t):
    r'NaN'
    t.value = nb.Value(nVal = nb.NullType.NaN)
    return t

def t_BAD_DATA(t):
    r'BAD_DATA'
    t.value = nb.Value(nVal = nb.NullType.BAD_DATA)
    return t

def t_BAD_TYPE(t):
    r'BAD_TYPE'
    t.value = nb.Value(nVal = nb.NullType.BAD_TYPE)
    return t

def t_OVERFLOW(t):
    r'OVERFLOW'
    t.value = nb.Value(nVal = nb.NullType.ERR_OVERFLOW)
    return t

def t_UNKNOWN_PROP(t):
    r'UNKNOWN_PROP'
    t.value = nb.Value(nVal = nb.NullType.UNKNOWN_PROP)
    return t

def t_DIV_BY_ZERO(t):
    r'DIV_BY_ZERO'
    t.value = nb.Value(nVal = nb.NullType.DIV_BY_ZERO)
    return t

def t_OUT_OF_RANGE(t):
    r'OUT_OF_RANGE'
    t.value = nb.Value(nVal = nb.NullType.OUT_OF_RANGE)
    return t

def t_FLOAT(t):
    r'-?\d+\.\d+'
    t.value = nb.Value(fVal = float(t.value))
    return t

def t_INT(t):
    r'-?\d+'
    t.value = nb.Value(iVal = int(t.value))
    return t

def t_BOOLEAN(t):
    r'(?i)true|false'
    v = nb.Value()
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
    t.value = nb.Value(sVal = t.lexer.string)
    t.lexer.begin('INITIAL')
    return t

def t_dstr_STRING(t):
    r'"'
    t.value = nb.Value(sVal = t.lexer.string)
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
    l = nb.List()
    if len(p) == 4:
        l.values = p[2]
    else:
        l.values = []
    p[0] = nb.Value(lVal = l)

def p_set(p):
    '''
        set : '{' list_items '}'
    '''
    s = nb.Set()
    s.values = set(p[2])
    p[0] = nb.Value(uVal = s)

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
    m = nb.Map()
    if len(p) == 4:
        m.kvs = p[2]
    else:
        m.kvs = {}
    p[0] = nb.Value(mVal = m)

def p_map_items(p):
    '''
        map_items : LABEL ':' expr
                  | STRING ':' expr
                  | map_items ',' LABEL ':' expr
                  | map_items ',' STRING ':' expr
    '''
    if len(p) == 4:
        k = p[1]
        if isinstance(k, nb.Value):
            k = k.get_sVal()
        p[0] = {k: p[3]}
    else:
        k = p[3]
        if isinstance(k, nb.Value):
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
    v = nb.Vertex(vid = vid, tags = tags)
    p[0] = nb.Value(vVal = v)

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
    tag = nb.Tag()
    tag.name = p[2]
    if len(p) == 4:
        tag.props = p[3].get_mVal()
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
    p[0] = nb.Value(eVal = e)

def p_edge_spec(p):
    '''
        edge_spec :
                  | '[' edge_rank edge_props ']'
                  | '[' ':' LABEL edge_rank edge_props ']'
                  | '[' STRING '-' '>' STRING edge_rank edge_props ']'
                  | '[' ':' LABEL STRING '-' '>' STRING edge_rank edge_props ']'
    '''
    e = nb.Edge()
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
        p[0] = p[1].get_mVal()

def p_path(p):
    '''
        path : '<' vertex steps '>'
             | '<' vertex '>'
    '''
    path = nb.Path()
    path.src = p[2].get_vVal()
    if len(p) == 5:
        path.steps = p[3]
    p[0] = nb.Value(pVal = path)

def p_steps(p):
    '''
        steps : edge vertex
              | steps edge vertex
    '''
    step = nb.Step()
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
    input = [
        '''EMPTY''',
        '''NULL''',
        '''NaN''',
        '''BAD_DATA''',
        '''BAD_TYPE''',
        '''OVERFLOW''',
        '''UNKNOWN_PROP''',
        '''DIV_BY_ZERO''',
        '''OUT_OF_RANGE''',
        '''123''',
        '''-123''',
        '''3.14''',
        '''-3.14''',
        '''true''',
        '''false''',
        ''' 'string' ''',
        '''"string"''',
        '''"string'substr'"''',
        ''' 'string"substr"' ''',
        '''[]''',
        '''[1,2,3]''',
        '''[<-[:e2{}]-,-[:e3{}]->]''',
        '''{1,2,3}''',
        '''{}''',
        '''{k1: 1, 'k2':true}''',
        '''()''',
        '''('vid')''',
        '''('vid':t)''',
        '''('vid':t:t)''',
        '''('vid':t{p1:0,p2:' '})''',
        '''('vid':t{p1:0,p2:' '}:t{})''',
        '''-->''',
        '''<--''',
        '''-[]->''',
        '''-[:e]->''',
        '''-[@-1]->''',
        '''-['1'->'2']->''',
        '''-[{}]->''',
        '''<-[:e{}]-''',
        '''-[:e{p1:0,p2:true}]->''',
        '''<-[:e@0{p1:0,p2:true}]-''',
        '''-[:e'1'->'2'{p1:0,p2:true}]->''',
        '''<-[:e@-1{p1:0,p2:true}]-''',
        '''<()>''',
        '''<()-->()<--()>''',
        '''<('v1':t{})>''',
        '''<('v1':t{})-[:e1{}]->('v2':t{})<-[:e2{}]-('v3':t{})>''',
    ]
    for s in input:
        v = parse(s)
        print("%64s -> %-5s: %s" % (s, v.__class__.__name__, str(v)))
    for spec in nb.Value.thrift_spec:
        if spec is not None:
            print(spec[2])
