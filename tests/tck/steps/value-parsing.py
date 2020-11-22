import os
import sys
from behave import *

this_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(this_dir + '/../utils/')
import nbv

# You could register functions that can be invoked from the parsing text
nbv.register_function('len', len)

@given('A set of string')
def step_impl(context):
    context.saved = context.table

@when('They are parsed as Nebula Value')
def step_impl(context):
    pass

@then('It must succeed')
def step_impl(context):
    values = []
    saved = context.saved.rows
    for row in saved:
        v = nbv.parse(row['string'])
        assert v != None, "Failed to parse `%s'" % row['string']
        values.append(v)
    context.values = values

@then('The type of the parsed value should be as expected')
def step_impl(context):
    n = len(context.values)
    saved = context.saved
    for i in range(n):
        actual = context.values[i].__class__.__name__
        expected = saved[i]['typename']
        assert actual == expected, \
                       "expected: %s, actual: %s" % (expected, actual)
