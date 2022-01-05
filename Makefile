install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

format:
	python -m black src

lint:
	pylint src tests --extension-pkg-whitelist=pyarrow &&\
	black src tests --check &&\
	yamllint src tests &&\
	bandit -r src -x ./tests -s B104

test:
	pytest -o log_cli=true

venv-install:
	python -m venv venv

venv-activate:
	. venv/bin/activate && exec bash