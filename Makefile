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
