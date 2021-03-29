<<<<<<< HEAD
# nebula-test 使用文档

- 安装pytest以及相关工具
  - pip3 install -r requirements.txt
- 设置环境变量
  - 设置结果输出目录
    - export NEBULA_TEST_LOGS_DIR=xxx
- 运行测试程序
  - 进入Nebula编译目录下的 tests 目录
  - ./ntr  query/stateless/
  - 可以通过 --rm_dir=false 取消删除创建的临时目录 /tmp/nebula-XXXXXXXX
  - 可以通过 --stop_nebula=false 取消停止 nebula 服务
  - 可以通过 --address=ip:port 连接自己的服务
- 测试如何编写
  - 例子：query/stateless/test_simple_query.py
  - 其中每一个功能 test case 都是一个类，而这个类必须继承 NebulaTestSuite.
  - 这里每一个 test_xxx 都是一个 case.
  - 这里特殊的两个方法则是 prepare 和 cleanup，分别是 case 的启动和退出的时候被调用.
  - 框架提供的函数
    - execute(ngql)
      - 用来执行非查询的一些语句，比如insert/show等等
    - execute(ngql)
      - 用来执行查询语句
    - check_resp_succeeded(resp)
      - 用来检测返回的值是否成功
    - check_resp_failed(resp)
      - 和check_resp_succeeded相反
    - search_result(col, rows, expect)
      - 用来检测返回的行是否和期望一致，这里行可以是乱序，并且列值支持正则, 使用正则的时候，需要设置下 is_regex=True, 并且所有列都要用正则
    - check_result(rows, expect)
      - 用来检测返回的行和期望一致，这里是行是按照严格顺序，并且列值支持正则, 需要设置下 is_regex=True, 并且所有列都要用正则
    - check_out_of_order_result
      - 用来检测返回的行是否和期望一致，行可以是乱序，不支持正则
    - check_empty_result
      - 用来检测结果是否为空
    - check_path_result(rows, expect)
      - 用来检测路径查询结果是否正确
  - 框架提供的变量
    - 这几个变量都是为了防止我们做查询或者插入时 schema 没有同步导致出错.
    - graph_delay
      - graphd和meta同步的时间间隔
    - storage_delay
      - storaged和meta同步的时间间隔
    - delay
      - 上面两个值之间的最大值
- 运行 pylint
    在 nebula 的源码目录执行: `pylint -j4 --rcfile=tests/.pylintrc tests`
=======
Nebula Graph Test Manual
========================

## Usage

### Build project

First of all, change directory to the root of `nebula-graph`, and build the whole project.

### Init environment

Nebula Test framework depends on some thirdparty libraries, such as [nebula-python](https://github.com/vesoft-inc/nebula-python), [reformat-gherkin](https://github.com/OneContainer/reformat-gherkin), pytest, [pytest-bdd](https://pytest-bdd.readthedocs.io/en/latest/) and so on.

So you should install all these dependencies before running test cases by:

```shell
$ make init-all
```

### Start nebula servers

Then run the following commands to start all nebula services built in above steps by GNU Make tool:

```shell
$ cd tests
$ make up
```

The target `up` in Makefile will select some random ports used by nebula, install all built necessary files to temporary folder which name format is like `server_2021-03-15T17-52-52` and start `metad/storaged/graphd` servers.

If your build directory is not `nebula-graph/build`, you should specify the `BUILD_DIR` parameter when up the nebula services:

```shell
$ make BUILD_DIR=/path/to/nebula/build/directory up
```

### Run all test cases

There are two classes of nebula graph test cases, one is built on pytest and another is built on TCK. We split them into different execution methods:

```shell
$ make test # run pytest cases
$ make tck  # run TCK cases
```

If you want to debug the `core` files when running tests, you can pass the `RM_DIR` parameter into `make` target to enable it, like:

```shell
$ make RM_DIR=false tck  # default value of RM_DIR is true
```

And if you want to debug only one test case, you should check the usage of `pytest` itself by `pytest --help`. For example, run the test cases related to `MATCH`, you can do it like:

```shell
$ pytest -k 'match' -m 'not skip' .
```

We also provide a parameter named `address` to allow these tests to connect to the nebula services maintained by yourself:

```shell
$ pytest --address="192.168.0.1:9669" -m 'not skip' .
```

You can use following commands to only rerun the test cases if they failed:

```shell
$ pytest --last-failed --gherkin-terminal-reporter --gherkin-terminal-reporter-expanded .
```

`gherkin-terminal-reporter` options will print the pytest report prettily.


### Stop nebula servers

Following command will stop the nebula servers started in above steps:

```shell
$ make down
```

If you want to stop some unused nebula processes, you can kill them by:

```shell
$ make kill
```

cleanup temporary files by:

```shell
$ make clean
```

## How to add test case

You can find all nebula test cases in [tck/features](tck/features) and some openCypher cases in [tck/openCypher/features](tck/openCypher/features). Some references about [TCK](https://github.com/opencypher/openCypher/tree/master/tck) may be what you need.

The test cases are organized in feature files and described in gherkin language. The structure of feature file is like following example:

```gherkin
Feature: Basic match

  Background:
    Given a graph with space named "nba"

  Scenario: Single node
    When executing query:
      """
      MATCH (v:player {name: "Yao Ming"}) RETURN v
      """
    Then the result should be, in any order, with relax comparison:
      | v            |
      | ("Yao Ming") |

  Scenario: One step
    When executing query:
      """
      MATCH (v1:player{name: "LeBron James"}) -[r]-> (v2)
      RETURN type(r) AS Type, v2.name AS Name
      """
    Then the result should be, in any order:
      | Type    | Name        |
      | "like"  | "Ray Allen" |
      | "serve" | "Cavaliers" |
      | "serve" | "Heat"      |
      | "serve" | "Lakers"    |
      | "serve" | "Cavaliers" |
```

Each feature file is composed of different scenarios which split test units into different parts. There are many steps in each scenario to define the inputs and outputs of test. These steps are started with following words:

- Given
- When
- Then

The table in `Then` step must have the first header line even if there's no data rows.

`Background` is the common steps of different scenarios. Scenarios will be executed in parallel.

### Format

In order to check your changed files for reviewers conveniently, please format your feature file before creating pull request. Try following command to do that:

```shell
$ make fmt
```
>>>>>>> 5bd53981... Fix version bug when packaging (#893)
