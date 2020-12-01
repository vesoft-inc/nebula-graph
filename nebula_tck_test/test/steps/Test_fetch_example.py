import time
import pytest
import sys
from pytest_bdd import scenarios, given, when, then, parsers
sys.path.insert(0,'../../nebula-clients/python/')
from nebula2.gclient.net import ConnectionPool
sys.path.insert(0,'../../utils/')

from text2dataset import text2dataset
from resultset_cmp_dataset import rst_cmp_dst_in_order
global resp 
resp = None


scenarios('../features/test_fetch_example.feature')

#@given("a empty space test02",target_fixture="step_001")
@given("a space nba", target_fixture="step_001")
def step_001(init_conn_pool,load_nba):
    connection_pool = init_conn_pool[0];
    client = connection_pool.get_session(init_conn_pool[1],init_conn_pool[2])
    assert client != None
    nGQL = "use nba;"
    client.execute(nGQL)
    return client


@when(parsers.parse("fetch player:\n{text}"))
def step_002(step_001,text):
    client = step_001
    global resp
    print(text)
    resp = client.execute(text)

@then(parsers.parse("the result should be:\n{text}"))
def step_003(step_001,text):
    client = step_001
    expect_dataset = text2dataset(text)
    global resp
    assert resp != None
    assert rst_cmp_dst_in_order(resp,expect_dataset) == True
