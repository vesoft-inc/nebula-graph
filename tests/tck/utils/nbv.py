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
    'LPAREN', 'RPAREN',
    'LBRACKET', 'RBRACKET',
    'LBRACE', 'RBRACE',
    'LT', 'GT',
    'COLON',
    'AT',
    'DASH',
    'COMMA',
    'LABEL',
)

t_LPAREN = r'\('
t_RPAREN = r'\)'
t_LBRACKET = r'\['
t_RBRACKET = r'\]'
t_LBRACE = r'\{'
t_RBRACE = r'\}'
t_LT = r'<'
t_GT = r'>'
t_AT = r'@'
t_DASH = r'-'
t_COLON = r':'
t_COMMA = r','
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
        list : LBRACKET list_item RBRACKET
             | LBRACKET RBRACKET
    '''
    if len(p) == 4:
        p[0] = p[2]
    else:
        p[0] = []

def p_set(p):
    '''
        set : LBRACE list_item RBRACE
    '''
    p[0] = set(p[2])

def p_list_item(p):
    '''
        list_item : expr
                  | list_item COMMA expr
    '''
    if len(p) == 2:
        p[0] = [p[1]]
    else:
        p[1].append(p[3])
        p[0] = p[1]

def p_map(p):
    '''
        map : LBRACE map_item RBRACE
            | LBRACE RBRACE
    '''
    if len(p) == 4:
        p[0] = p[2]
    else:
        p[0] = {}


def p_map_item(p):
    '''
        map_item : LABEL COLON expr
                 | STRING COLON expr
                 | map_item COMMA LABEL COLON expr
    '''
    if len(p) == 4:
        p[0] = {p[1]: p[3]}
    else:
        p[1][p[3]] = p[5]
        p[0] = p[1]

def p_node(p):
    '''
        node : LPAREN STRING tag_list RPAREN
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
        tag : COLON LABEL map
    '''
    p[0] = Tag(p[2], p[3])

def p_edge(p):
    '''
        edge : DASH LBRACKET COLON LABEL map RBRACKET DASH
             | DASH LBRACKET COLON LABEL map RBRACKET DASH GT
             | LT DASH LBRACKET COLON LABEL map RBRACKET DASH
             | LT DASH LBRACKET COLON LABEL map RBRACKET DASH GT
             | DASH LBRACKET COLON LABEL AT INT map RBRACKET DASH
             | DASH LBRACKET COLON LABEL AT INT map RBRACKET DASH GT
             | LT DASH LBRACKET COLON LABEL AT INT map RBRACKET DASH
             | LT DASH LBRACKET COLON LABEL AT INT map RBRACKET DASH GT
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
        path : LT node steps GT
             | LT node GT
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
        function : LABEL LPAREN list_item RPAREN
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
