URLS ?= https://blog.codeasap.pl
FQDN ?= -w blog.codeasap.pl

TMP_DIR ?= .ignore/tmp

all:

dirs:
	mkdir -p $(TMP_DIR)

setup:
	pip install -r requirements.txt
	pip install -r requirements-dev.txt

clean:
	find . -type d ! -name '.venv' -name __pycache__ | xargs rm -rfv

doxygen:
	mkdir -p docs/doxygen
	doxygen Doxyfile

pep8:
	flake8 crawler.py
	flake8 tests/*.py

test: pep8
	python3 -m unittest discover -v .

test-pytest: pep8
	pytest -vs tests

coverage: dirs
	coverage run -m unittest discover -vs .
	coverage html -d $(TMP_DIR)/coverage

run:
	./crawler.py -C $(FQDN) $(URLS)
