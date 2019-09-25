
# https://gist.github.com/paulfurley/869df266ef36231014f434686202eb9f
# Put *unversioned* requirements in `requirements-to-freeze.txt` as described below.
# `requirements.txt` will be automatically generated from `pip freeze`
# https://www.kennethreitz.org/essays/a-better-pip-workflow

venv/bin/activate: requirements-to-freeze.txt
	rm -rf venv/
	test -f venv/bin/activate || virtualenv -p $(shell which python3) venv
	. venv/bin/activate ;\
	pip install -Ur requirements-to-freeze.txt ;\
	pip freeze | sort > requirements.txt
	touch venv/bin/activate  # update so it's as new as requirements-to-freeze.txt

venv_lint: venv/bin/activate
	. venv/bin/activate ; \
	venv/bin/flake8

venv_test: venv/bin/activate
	. venv/bin/activate ; \
	venv/bin/pytest -s

lint:
	flake8

test:
	pytest -s
