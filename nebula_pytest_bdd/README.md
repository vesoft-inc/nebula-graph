1、nebula-pytest-bdd使用文档
安装pytest相关工具
pip3 install -r requirements.txt

2、下载客户端代码到目录nebula_pytest_bdd （后续会统一到requirements里面）
git clone git@github.com:vesoft-inc/nebula-clients.git


3、case 添加方法
case 统一在test 目录下
features目录：
    按照Gherkin自然语言描述：GIVEN WHEN THEN
steps.py目录：
    具体将features文件里面的内容进行翻译成python代码运行

4、运行前
   在nebula 工程目录下，创建build 并编译完成

5、运行命令
   python3 nebula_pytest_bdd.py
   运行完成后当前目录生存report.html

6、文件说明
   pytest.ini是具体的相关pytest的运行参数
   nebula.json 是保存环境启动相关信息
   conftest.py 是运行pytest_bdd case的一些公共信息：创建nba students

