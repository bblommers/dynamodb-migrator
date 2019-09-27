
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

release: check-version check-local-changes venv/bin/activate
	@. venv/bin/activate ; \
	echo Versioning... ; \
	awk '{sub(/'[0-9.]+'/,"${VERSION}")}1' src/migrator/__init__.py > temp.txt && mv temp.txt src/migrator/__init__.py; \
	git add src/migrator/__init__.py
	git commit -m "Release ${VERSION}"
	echo Tagging... ; \
	git tag -a ${VERSION} -m "Release of version ${VERSION}"
	echo Pushing... ; \
	git push --follow-tags
	echo "Packaging..."; \
	python3 setup.py sdist bdist_wheel > /dev/null 2>&1; \
	echo "Releasing to PyPi..."; \
	python3 -m twine upload dist/*${VERSION}*

check-version:
ifndef VERSION
	$(error VERSION ${VERSION} is undefined)
endif

check-local-changes:
	LOCAL_CHANGES=0
	git diff --no-ext-diff --quiet --exit-code && LOCAL_CHANGES=1
	if [ $LOCAL_CHANGES = 0 ]; then
		LOCAL_CHANGES=`git ls-files --exclude-standard --others| wc -l`
	fi
	if ! [ $LOCAL_CHANGES = 0 ]; then
		$(error You have local changes! Please checkout from master)
	fi
