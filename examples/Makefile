venv/bin/activate:
	rm -rf venv/
	test -f venv/bin/activate || virtualenv -p $(shell which python3) venv
	. venv/bin/activate ;\
	pip install -r requirements.txt
	touch venv/bin/activate

customer_table: venv/bin/activate
	@. venv/bin/activate ; \
	python customer_table.py