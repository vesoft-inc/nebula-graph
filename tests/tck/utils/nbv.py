import ply.lex as lex
import ply.yacc as yacc

states = (
    ('sstr', 'exclusive'),
    ('dstr', 'exclusive'),
)

tokens = (
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

def t_FLOAT(t):
    r'-?\d+\.\d+'
    t.value = float(t.value)
    return t

def t_INT(t):
    r'-?\d+'
    t.value = int(t.value)
    return t

def t_BOOLEAN(t):
    r'(?i)true|false'
    if t.value.lower() == 'true':
        t.value = True
    else:
        t.value = False
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
    t.value = t.lexer.string
    t.lexer.begin('INITIAL')
    return t

def t_dstr_STRING(t):
    r'"'
    t.value = t.lexer.string
    t.lexer.begin('INITIAL')
    return t

def t_ANY_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)

class Node:
    def __init__(self, vid, tags):
        self.vid = vid
        self.tags = tags

    def __str__(self):
        s = "('%s'" % self.vid
        for tag in self.tags:
            s += " %s" % str(tag)
        s += ")"
        return s

    def __repr__(self):
        return self.__str__()

class Tag:
    def __init__(self, name, props):
        self.name = name
        self.props = props

    def __str__(self):
        return ":%s %s" % (self.name, self.props)

    def __repr__(self):
        return self.__str__()

class Edge:
    def __init__(self, name, rank, props, direct):
        self.name = name
        self.rank = rank
        self.props = props
        self.direct = direct

    def __str__(self):
        s = "-[:%s @%d %s]-" % (self.name, self.rank, str(self.props))
        if self.direct == ">":
            return s + ">"
        elif self.direct == "<":
            return "<" + s
        else:
            return s

    def __repr__(self):
        return self.__str__()

class Path:
    def __init__(self, head, steps):
        self.head = head
        self.steps = steps

    def __str__(self):
        s = "<%s" % str(self.head)
        if self.steps is not None:
            for step in self.steps:
                s += str(step)
        s += ">"
        return s

    def __repr__(self):
        return self.__str__()

class Step:
    def __init__(self, edge, node):
        self.edge = edge
        self.node = node

    def __str__(self):
        return "%s%s" % (str(self.edge), str(self.node))

    def __repr__(self):
        return self.__str__()

def p_expr(p):
    '''expr : INT
            | FLOAT
            | BOOLEAN
            | STRING
            | list
            | set
            | map
            | node
            | edge
            | path
            | function
    '''
    p[0] = p[1]

def p_list(p):
    '''
        list : '[' list_item ']'
             | '[' ']'
    '''
    if len(p) == 4:
        p[0] = p[2]
    else:
        p[0] = []

def p_set(p):
    '''
        set : '{' list_item '}'
    '''
    p[0] = set(p[2])

def p_list_item(p):
    '''
        list_item : expr
                  | list_item ',' expr
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[3])
        p[0] = p[1]

def p_map(p):
    '''
        map : '{' map_item '}'
            | '{' '}'
    '''
    if len(p) == 4:
        p[0] = p[2]
    else:
        p[0] = {}


def p_map_item(p):
    '''
        map_item : LABEL ':' expr
                 | STRING ':' expr
                 | map_item ',' LABEL ':' expr
    '''
    if len(p) == 4:
        p[0] = {p[1]: p[3]}
    else:
        p[1][p[3]] = p[5]
        p[0] = p[1]

def p_node(p):
    '''
        node : '(' STRING tag_list ')'
    '''
    p[0] = Node(p[2], p[3])

def p_tag_list(p):
    '''
        tag_list : tag
                 | tag_list tag
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[2])
        p[0] = p[1]

def p_tag(p):
    '''
        tag : ':' LABEL map
    '''
    p[0] = Tag(p[2], p[3])

def p_edge(p):
    '''
        edge : '-' '[' ':' LABEL map ']' '-'
             | '-' '[' ':' LABEL map ']' '-' '>'
             | '<' '-' '[' ':' LABEL map ']' '-'
             | '<' '-' '[' ':' LABEL map ']' '-' '>'
             | '-' '[' ':' LABEL '@' INT map ']' '-'
             | '-' '[' ':' LABEL '@' INT map ']' '-' '>'
             | '<' '-' '[' ':' LABEL '@' INT map ']' '-'
             | '<' '-' '[' ':' LABEL '@' INT map ']' '-' '>'
    '''
    if len(p) == 8:
        p[0] = Edge(p[4], 0, p[5], None)
    elif len(p) == 9:
        if p[1] == '<':
            p[0] = Edge(p[5], 0, p[6], '<')
        else:
            p[0] = Edge(p[4], 0, p[5], '>')
    elif len(p) == 10:
        if p[1] == '<':
            p[0] = Edge(p[5], 0, p[6], None)
        else:
            p[0] = Edge(p[4], p[6], p[7], None)
    elif len(p) == 11:
        if p[1] == '<':
            p[0] = Edge(p[5], p[7], p[8], '<')
        else:
            p[0] = Edge(p[4], p[6], p[7], '>')
    elif len(p) == 12:
            p[0] = Edge(p[5], p[7], p[8], None)

def p_path(p):
    '''
        path : '<' node steps '>'
             | '<' node '>'
    '''
    if len(p) == 5:
        p[0] = Path(p[2], p[3])
    else:
        p[0] = Path(p[2], None)

def p_steps(p):
    '''
        steps : edge node
              | steps edge node
    '''
    if len(p) == 1:
        p[0] = []
    elif len(p) == 3:
        p[0] = [Step(p[1], p[2])]
    else:
        p[1].append(Step(p[2], p[3]))
        p[0] = p[1]

def p_function(p):
    '''
        function : LABEL '(' list_item ')'
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
        '''[-[:e1{}]-,<-[:e2{}]-,-[:e3{}]->]''',
        '''{1,2,3}''',
        '''{}''',
        '''{k1: 1, k2:true}''',
        '''('vid':t{p1:0,p2:' '})''',
        '''-[:e{p1:0,p2:true}]-''',
        '''<-[:e@0{p1:0,p2:true}]-''',
        '''-[:e{p1:0,p2:true}]->''',
        '''<-[:e@-1{p1:0,p2:true}]->''',
        '''<('v1':t{})>''',
        '''<('v1':t{})-[:e1{}]-('v2':t{})<-[:e2{}]->('v3':t{})>''',
    ]
    for s in input:
        v = parse(s)
        print("%64s -> %-5s: %s" % (s, v.__class__.__name__, str(v)))
