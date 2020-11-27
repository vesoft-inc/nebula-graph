import time
import pytest
import sys
from pytest_bdd import scenarios, given, when, then, parsers
sys.path.insert(0,'../nebula-clients/python/')
from nebula2.gclient.net import ConnectionPool

global resp 
resp = None

scenarios('../features/query.feature')
@given(parsers.parse("create a space {spacename} and a vertex {player}, a set of person"),target_fixture="step_001")
def step_001(init_client,load_nba,load_students,spacename,player):
    connection_pool = init_client[0];
    client = connection_pool.get_session(init_client[1],init_client[2])
    assert client != None
    nGQL = "drop space if exists " + spacename + ";"
    print(nGQL)
    client.execute(nGQL)
    time.sleep(5)
    nGQL = "create space if not exists " + spacename + ";"
    print(nGQL)
    client.execute(nGQL)
    time.sleep(5)
    nGQL = "use "+ spacename + "; create tag "+ player +"(name string, age int);"
    client.execute(nGQL)
    print(nGQL)
    time.sleep(5)
    nGQL = 'INSERT VERTEX person(name, age) VALUES "Tom":("Tom",12)'
    print(nGQL)
    client.execute(nGQL)
    print(nGQL)
    return client

@when("execute query")
def step_002(step_001):
    client = step_001
    query = 'FETCH PROP ON person "Tom"'
    global resp
    resp = client.execute(query)

@then("the result code shoud be")
def step_003():
    global resp
    code = resp.error_code()
    print(type(code))
    assert code == 0


