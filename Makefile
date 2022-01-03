install:
	pip install --upgrade pip &&\
	pip install -r requirements.txt

format:
	python -m black src

lint:
	pylint src --extension-pkg-whitelist=pyarrow &&\
	black src --check &&\
	bandit -r src -x ./tests -s B104

test:
	pytest