Nebula-pytest-bdd use guide
1、Deploy pytest tools
pip3 install -r requirements.txt

download repo to install:
git clone https://github.com/vesoft-inc/nebula-python.git
cd  nebula-python
python3 setup.py install

2、How to add cases
In test directory have two directories :
features directory:
    According Gherkin to describe case step：
        for example:
            GIVEN ...

            WHEN ...

            THEN ...

steps directory：
    Convert the step in .feature file to python code

3、Before run
   In project directory,make buid directory to buid all of the project program  


4、How to run 
   python3 nebula_pytest_bdd.py

5、The file's function
   pytest.ini : config the args about pytest
   nebula.json : config the args about connect nebula
   conftest.py : implement common function serve to cases

