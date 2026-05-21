PYTHON := .venv/bin/python

.PHONY: setup lab notebook freeze clean test lint format

setup:
	bash setup.sh

lab:
	$(PYTHON) -m jupyter lab

notebook:
	$(PYTHON) -m jupyter notebook

freeze:
	uv pip freeze --python $(PYTHON) > requirements.lock.txt

clean:
	rm -rf .venv __pycache__ */__pycache__ .ipynb_checkpoints */.ipynb_checkpoints

test:
	$(PYTHON) -m pytest tests/ -v

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .
