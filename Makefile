.PHONY: setup lab notebook freeze clean

setup:
	bash setup.sh

lab:
	. .venv/bin/activate && jupyter lab

notebook:
	. .venv/bin/activate && jupyter notebook

freeze:
	. .venv/bin/activate && pip freeze > requirements.lock.txt

clean:
	rm -rf .venv __pycache__ */__pycache__ .ipynb_checkpoints */.ipynb_checkpoints

test:
	. .venv/bin/activate && pytest tests/ -v

lint:
	. .venv/bin/activate && ruff check .
	. .venv/bin/activate && ruff format --check .

format:
	. .venv/bin/activate && ruff format .
	. .venv/bin/activate && ruff check --fix .