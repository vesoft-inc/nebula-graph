# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

.PHONY: format check init clean

PYPI_MIRROR = https://mirrors.aliyun.com/pypi/simple/

install-deps:
	pip3 install --user -U setuptools -i $(PYPI_MIRROR)
	pip3 install --user -r requirements.txt -i $(PYPI_MIRROR)

install-nebula-py: install-deps
	git clone --branch master https://github.com/vesoft-inc/nebula-python
	cd nebula-python && python3 setup.py install --user && cd ..
	rm -rf nebula-python

gherkin-fmt: install-deps
	pip3 install poetry
	git clone --branch master https://github.com/OneContainer/reformat-gherkin
	cd reformat-gherkin && poetry build
	pip3 install --user reformat_gherkin-2.0.2-py3-none-any.whl
	cd .. && rm -rf reformat-gherkin

init: clean gherkin-fmt install-nebula-py
	@echo "Install nebula test dependencies"

format:
	find tck/features -type f -iname '*.feature' -exec reformat-gherkin {} \;

check:
	find tck/features -type f -iname "*.feature" -exec sh -c 'for f; do reformat-gherkin --check "$$f" || exit 1; done' sh {} +

clean:
	@rm -rf nebula-python reformat-gherkin .pytest .pytest_cache
