# Copyright (c) 2018 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License,
# attached with Common Clause Condition 1.0, found in the LICENSES directory.

.PHONY: format check init clean test

PYPI_MIRROR = https://mirrors.aliyun.com/pypi/simple/
CURR_DIR = $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

install-deps:
	pip3 install --user -U setuptools -i $(PYPI_MIRROR)
	pip3 install --user -r $(CURR_DIR)/requirements.txt -i $(PYPI_MIRROR)

install-nebula-py: install-deps
	git clone --branch master https://github.com/vesoft-inc/nebula-python $(CURR_DIR)/nebula-python
	cd $(CURR_DIR)/nebula-python && python3 setup.py install --user
	rm -rf $(CURR_DIR)/nebula-python

gherkin-fmt: install-deps
	pip3 install --user poetry
	git clone --branch master https://github.com/OneContainer/reformat-gherkin $(CURR_DIR)/reformat-gherkin
	cd $(CURR_DIR)/reformat-gherkin && poetry build
	pip3 install --user $(CURR_DIR)/reformat-gherkin/dist/reformat_gherkin*.whl
	rm -rf $(CURR_DIR)/reformat-gherkin

init: clean gherkin-fmt install-nebula-py
	@echo "Install nebula test dependencies"

fmt:
	find $(CURR_DIR)/tck/features -type f -iname '*.feature' -exec reformat-gherkin {} \;

check:
	find $(CURR_DIR)/tck/features -type f -iname "*.feature" -exec sh -c 'for f; do reformat-gherkin --check "$$f" || exit 1; done' sh {} +

test: format
	cd $(CURR_DIR) && pytest -n8 --dist=loadfile -m "not skip" --build_dir=$(CURR_DIR)/../build

clean:
	@rm -rf $(CURR_DIR)/nebula-python $(CURR_DIR)/reformat-gherkin $(CURR_DIR)/.pytest $(CURR_DIR)/.pytest_cache
